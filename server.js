const express = require('express');
const { WebSocketServer } = require('ws');
const Redis = require('ioredis');
const path = require('path');
const http = require('http');
const os = require('os');
const crypto = require('crypto');
const promClient = require('prom-client');
const k8s = require('@kubernetes/client-node');

// ── Kubernetes Client ────────────────────────────────────────────────────────
const kc = new k8s.KubeConfig();
if (process.env.KUBERNETES_SERVICE_HOST) {
  kc.loadFromCluster();
} else {
  kc.loadFromDefault();
}
const k8sApi = kc.makeApiClient(k8s.CoreV1Api);
const k8sAppsApi = kc.makeApiClient(k8s.AppsV1Api);
const k8sWatch = new k8s.Watch(kc);

// ── Prometheus metrics ───────────────────────────────────────────────────────
const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register });

const activeUsersGauge = new promClient.Gauge({
  name: 'websocket_active_users_total',
  help: 'Total number of active users tracked in Redis',
  registers: [register],
});

const activeConnectionsGauge = new promClient.Gauge({
  name: 'websocket_active_connections_total',
  help: 'Total number of open WebSocket connections on this pod',
  registers: [register],
});

const counterIncrementTotal = new promClient.Counter({
  name: 'websocket_counter_increments_total',
  help: 'Total number of counter increment operations',
  labelNames: ['pod'],
  registers: [register],
});

const redisFailoverTotal = new promClient.Counter({
  name: 'redis_failover_events_total',
  help: 'Total number of Redis topology change events detected',
  registers: [register],
});

const wsMessageTotal = new promClient.Counter({
  name: 'websocket_messages_received_total',
  help: 'Total number of WebSocket messages received',
  labelNames: ['type', 'pod'],
  registers: [register],
});

// ── App bootstrap ────────────────────────────────────────────────────────────
const app = express();
const PORT = 3000;
const HOSTNAME = os.hostname();

function logEvent(level, eventName, data = {}) {
  const { request_id, ...rest } = data;
  const payload = {
    timestamp: new Date().toISOString(),
    level,
    event: eventName,
    pod: HOSTNAME,
    ...rest
  };
  if (request_id) payload.request_id = request_id;
  console.log(JSON.stringify(payload));
}

app.use(express.static(path.join(__dirname, 'public')));

app.get('/health', (req, res) => {
  if (redisClient.status === 'ready') {
    res.status(200).json({ status: 'ok', redis: 'connected', pod: HOSTNAME });
  } else {
    res.status(503).json({ status: 'degraded', redis: redisClient.status, pod: HOSTNAME });
  }
});

app.get('/metrics', async (req, res) => {
  try {
    const liveActiveUsers = await redisClient.scard('active_users').catch(() => 0);
    activeUsersGauge.set(liveActiveUsers);
    activeConnectionsGauge.set(wss ? wss.clients.size : 0);
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  } catch (err) {
    res.status(500).end(err.message);
  }
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ── Kubernetes Watchers ──────────────────────────────────────────────────────
const podCache = new Map();
const deploymentCache = new Map();
const serviceCache = new Map();

function startK8sWatch() {
  const watchResource = (resourcePath, cacheMap, resourceName) => {
    const doWatch = () => {
      k8sWatch.watch(resourcePath, {},
        (type, apiObj, watchObj) => {
          if (type === 'ADDED' || type === 'MODIFIED') {
            cacheMap.set(apiObj.metadata.name, apiObj);
          } else if (type === 'DELETED') {
            cacheMap.delete(apiObj.metadata.name);
          }
          broadcastClusterState();
        },
        (err) => {
          console.error(`Watch ended for ${resourceName}:`, err);
          setTimeout(doWatch, 2000);
        }
      );
    };
    doWatch();
  };
  
  watchResource('/api/v1/namespaces/websocket-app/pods', podCache, 'Pods');
  watchResource('/apis/apps/v1/namespaces/websocket-app/deployments', deploymentCache, 'Deployments');
  watchResource('/api/v1/namespaces/websocket-app/services', serviceCache, 'Services');
}
startK8sWatch();

// ── Redis Sentinel discovery ─────────────────────────────────────────────────
let sentinelHosts = [
  { host: 'sentinel-1', port: 26379 },
  { host: 'sentinel-2', port: 26379 },
  { host: 'sentinel-3', port: 26379 },
];

if (process.env.REDIS_SENTINEL_HOSTS) {
  sentinelHosts = process.env.REDIS_SENTINEL_HOSTS.split(',').map(h => ({
    host: h.trim(),
    port: 26379,
  }));
}

const redisOptions = {
  sentinels: sentinelHosts,
  name: 'mymaster',
  reconnectOnError: () => true,
  retryStrategy: (times) => Math.min(times * 100, 2000),
  maxRetriesPerRequest: null,
};

const redisClient = new Redis(redisOptions);
const redisSubscriber = new Redis(redisOptions);

let currentMasterName = 'Unknown';
let currentReplicaName = 'Unknown';
let currentReplicaStatus = 'Disconnected/None';
let lastTopologyChange = Date.now();
let sentinelLeader = 'Unknown';
let connectedSentinels = 0;
let redisLatency = 0;
let replicationLag = '0 ms';
let totalFailovers = 0;

redisClient.on('error', (err) => logEvent('error', 'redis_error', { operation: 'background_client', error: err.message }));
redisSubscriber.on('error', (err) => logEvent('error', 'redis_error', { operation: 'background_subscriber', error: err.message }));
redisClient.on('reconnecting', () => logEvent('warn', 'redis_connection_lost', { msg: 'Redis connection lost. Asking Sentinel...' }));

async function updateTopologyInfo() {
  try {
    let sentinelClient = null;
    let connectedHost = null;
    
    // Find active sentinel
    for (const s of sentinelHosts) {
        try {
            const client = new Redis({ host: s.host, port: s.port, maxRetriesPerRequest: 1, commandTimeout: 1000 });
            await client.ping();
            sentinelClient = client;
            connectedHost = s.host;
            break;
        } catch(e) { /* ignore */ }
    }
    
    if (!sentinelClient) throw new Error("No sentinels available");

    const masterAddr = await sentinelClient.send_command('SENTINEL', ['get-master-addr-by-name', 'mymaster']);
    const masterIp = masterAddr ? masterAddr[0] : null;

    const replicasInfo = await sentinelClient.send_command('SENTINEL', ['replicas', 'mymaster']);
    const sentinelsInfo = await sentinelClient.send_command('SENTINEL', ['sentinels', 'mymaster']);
    
    // Check redis latency
    const pingStart = performance.now();
    await redisClient.ping();
    redisLatency = parseFloat((performance.now() - pingStart).toFixed(2));
    
    // Sentinel info processing
    sentinelLeader = connectedHost;
    connectedSentinels = sentinelsInfo.length + 1; // including self
    
    sentinelClient.disconnect();

    let newMasterName = 'Unknown';
    let newReplicaName = 'Unknown';
    let newReplicaStatus = 'Disconnected/None';
    
    // Cross-reference IP with Kubernetes Pod Watch Cache and Service Cache
    for (const [name, pod] of podCache.entries()) {
        if (pod.status && pod.status.podIP === masterIp) newMasterName = name;
    }
    if (newMasterName === 'Unknown') {
        for (const [name, svc] of serviceCache.entries()) {
            if (svc.spec && svc.spec.clusterIP === masterIp) newMasterName = name;
        }
    }
    if (newMasterName === 'Unknown' && masterIp) newMasterName = masterIp; // fallback

    if (replicasInfo && replicasInfo.length > 0) {
      const replicaData = replicasInfo[0];
      const rMap = {};
      for (let i = 0; i < replicaData.length; i += 2) rMap[replicaData[i]] = replicaData[i + 1];

      for (const [name, pod] of podCache.entries()) {
          if (pod.status && pod.status.podIP === rMap.ip) newReplicaName = name;
      }
      if (newReplicaName === 'Unknown') {
          for (const [name, svc] of serviceCache.entries()) {
              if (svc.spec && svc.spec.clusterIP === rMap.ip) newReplicaName = name;
          }
      }
      if (newReplicaName === 'Unknown') newReplicaName = rMap.ip;

      if (rMap.flags.includes('s_down') || rMap.flags.includes('o_down')) {
        newReplicaStatus = 'Disconnected';
      } else if (rMap['master-link-status'] === 'ok') {
        newReplicaStatus = 'Connected';
      } else {
        newReplicaStatus = 'Syncing';
      }
    }

    if (newMasterName !== currentMasterName || newReplicaName !== currentReplicaName) {
      logEvent('info', 'redis_topology_update', { primary: newMasterName, replica: newReplicaName, replication_status: newReplicaStatus });
      
      if (currentMasterName !== 'Unknown' && newMasterName !== 'Unknown' && currentMasterName !== newMasterName) {
          logEvent('info', 'redis_failover', { old_primary: currentMasterName, new_primary: newMasterName, detected_by: sentinelLeader, failover_time_ms: Date.now() - lastTopologyChange });
          redisFailoverTotal.inc();
          totalFailovers++;
      }
      lastTopologyChange = Date.now();
      currentMasterName = newMasterName;
      currentReplicaName = newReplicaName;
      broadcastPrimaryInfo();
    }
    currentReplicaStatus = newReplicaStatus;
  } catch (err) {
    logEvent('error', 'topology_polling_failed', { message: err.message });
  }
}

redisClient.on('ready', async () => {
  await updateTopologyInfo();
});

function broadcastPrimaryInfo() {
  const broadcastMessage = JSON.stringify({ type: 'PRIMARY_INFO', primary: currentMasterName });
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(broadcastMessage);
  });
}

function broadcastClusterState() {
    const pods = Array.from(podCache.values()).map(p => ({
        name: p.metadata.name,
        phase: (p.metadata.labels && p.metadata.labels.stopped === 'true') ? 'Stopped' : p.status.phase,
        podIP: p.status.podIP,
        restarts: p.status.containerStatuses ? p.status.containerStatuses.reduce((acc, c) => acc + c.restartCount, 0) : 0,
        image: p.spec.containers[0].image
    }));
  const deployments = Array.from(deploymentCache.values()).map(d => ({
      name: d.metadata.name,
      replicas: d.spec.replicas,
      readyReplicas: d.status.readyReplicas
  }));
  
  const adminMessage = JSON.stringify({ type: 'CLUSTER_STATE_UPDATE', payload: { pods, deployments } });
  wss.clients.forEach((client) => {
    if (client.readyState === 1 && client.isAdmin) client.send(adminMessage);
  });
}

// ── Stale connection cleanup ─────────────────────────────────────────────────
setInterval(async () => {
  try {
    const cutoff = Date.now() - 15000;
    const staleConnections = await redisClient.zrangebyscore('heartbeats', '-inf', cutoff);
    if (staleConnections.length > 0) {
      logEvent('info', 'stale_connections_cleaned', { count: staleConnections.length });
      for (const entry of staleConnections) {
        const [connId, username, hostname] = entry.split('|');
        if (connId && username) await removeConnection(connId, username, hostname || 'Unknown');
      }
      await redisClient.zremrangebyscore('heartbeats', '-inf', cutoff);
    }
  } catch {}
}, 10000);

async function removeConnection(connectionId, username, hostname) {
  await redisClient.srem(`user_connections:${username}`, connectionId);
  await redisClient.hdel('user_container_map', connectionId);

  const remaining = await redisClient.scard(`user_connections:${username}`);
  let count = await redisClient.scard('active_users');

  if (remaining === 0) {
    await redisClient.srem('active_users', username);
    count = await redisClient.scard('active_users');
    await redisClient.publish('admin_events', JSON.stringify({ type: 'USER_LEFT', username, hostname, count }));
  }
  return count;
}

// ── Operations & Chaos ───────────────────────────────────────────────────────
async function launchStressPod(type) {
    const podName = `stress-${type}-${Date.now()}`;
    const podManifest = {
        metadata: { name: podName, namespace: 'websocket-app', labels: { app: 'stress-test' } },
        spec: {
            securityContext: {
                seccompProfile: { type: 'RuntimeDefault' },
                runAsNonRoot: true,
                runAsUser: 1000
            },
            containers: [{
                name: 'stress',
                image: 'busybox',
                command: ['/bin/sh', '-c'],
                args: type === 'cpu' 
                    ? ['timeout 60s /bin/sh -c "while true; do :; done"'] 
                    : ['timeout 60s /bin/sh -c "head -c 256m /dev/zero > /dev/null"'],
                securityContext: {
                    allowPrivilegeEscalation: false,
                    capabilities: { drop: ['ALL'] }
                }
            }],
            restartPolicy: 'Never'
        }
    };
    await k8sApi.createNamespacedPod('websocket-app', podManifest);
}

// ── Server start ─────────────────────────────────────────────────────────────
async function startServer() {
  await redisSubscriber.subscribe('counter_updates');
  await redisSubscriber.subscribe('admin_events');

  redisSubscriber.on('message', (channel, message) => {
    if (channel === 'counter_updates') {
      const newValue = parseInt(message, 10);
      const broadcastMessage = JSON.stringify({ type: 'UPDATE_COUNTER', value: newValue });
      wss.clients.forEach((client) => {
        if (client.readyState === 1 && !client.isAdmin) client.send(broadcastMessage);
      });
    } else if (channel === 'admin_events') {
      const adminMessage = JSON.stringify({ type: 'ADMIN_EVENT', payload: JSON.parse(message) });
      wss.clients.forEach((client) => {
        if (client.readyState === 1 && client.isAdmin) client.send(adminMessage);
      });
    }
  });

  // Background polling for Redis Topology info (this is safe to poll as it updates state based on Sentinel)
  setInterval(async () => {
    try {
      await updateTopologyInfo();
      const userContainerMap = await redisClient.hgetall('user_container_map');
      const mapped = {};
      const activeContainers = new Set();
      
      let totalConnections = 0;
      for (const [, data] of Object.entries(userContainerMap)) {
        try {
          const { u: user, h: host } = JSON.parse(data);
          if (!mapped[user]) mapped[user] = new Set();
          mapped[user].add(host);
          activeContainers.add(host);
          totalConnections++;
        } catch {}
      }

      const finalMap = {};
      for (const [u, hosts] of Object.entries(mapped)) {
        finalMap[u] = Array.from(hosts);
      }

      await redisClient.publish('admin_events', JSON.stringify({
        type: 'INFRA_UPDATE',
        activeContainers: activeContainers.size,
        totalConnections,
        userMap: finalMap,
        redisMaster: currentMasterName,
        redisReplica: currentReplicaName,
        replicaStatus: currentReplicaStatus,
        sentinelLeader,
        connectedSentinels,
        redisLatency,
        replicationLag,
        totalFailovers,
        lastTopologyChange,
      }));
    } catch {}
  }, 2000);

  wss.on('connection', async (ws) => {
    let connectionId = null;
    let username = null;
    ws.isAdmin = false;
    let sessionStartTime = null;

    ws.send(JSON.stringify({ type: 'CONTAINER_INFO', hostname: HOSTNAME }));
    ws.send(JSON.stringify({ type: 'PRIMARY_INFO', primary: currentMasterName }));

    const syncCounter = async () => {
      try {
        let currentCounter = await redisClient.get('global_counter');
        if (currentCounter === null) { currentCounter = 0; await redisClient.set('global_counter', 0); }
        ws.send(JSON.stringify({ type: 'UPDATE_COUNTER', value: parseInt(currentCounter) }));
      } catch {}
    };
    await syncCounter();

    ws.on('message', async (message) => {
      try {
        const parsedMessage = JSON.parse(message);
        if (parsedMessage.type === 'ping') {
            ws.send(JSON.stringify({ type: 'pong' }));
            return;
        }
        
        process.stdout.write("RAW_MSG_TYPE: " + parsedMessage.type + "\n");
        console.log("RECEIVED MSG:", parsedMessage);

        const reqStart = performance.now();
        wsMessageTotal.inc({ type: parsedMessage.type || 'unknown', pod: HOSTNAME });

        if (parsedMessage.type === 'admin_join') {
          ws.isAdmin = true;
          const count = await redisClient.scard('active_users');
          ws.send(JSON.stringify({ type: 'ADMIN_EVENT', payload: { type: 'COUNT_UPDATE', count } }));
          broadcastClusterState(); // Send initial state
        } else if (parsedMessage.type === 'ping') {
          if (connectionId && username) await redisClient.zadd('heartbeats', Date.now(), `${connectionId}|${username}|${HOSTNAME}`);
        } else if (parsedMessage.type === 'sync') {
          await syncCounter();
        } else if (parsedMessage.type === 'join') {
          sessionStartTime = Date.now();
          username = parsedMessage.username;
          connectionId = crypto.randomUUID();
          await redisClient.sadd(`user_connections:${username}`, connectionId);
          const isFirstConn = await redisClient.sadd('active_users', username);
          await redisClient.hset('user_container_map', connectionId, JSON.stringify({ u: username, h: HOSTNAME }));
          await redisClient.zadd('heartbeats', Date.now(), `${connectionId}|${username}|${HOSTNAME}`);
          const count = await redisClient.scard('active_users');
          if (isFirstConn === 1) {
            await redisClient.publish('admin_events', JSON.stringify({ type: 'USER_JOINED', username, hostname: HOSTNAME, count, latency: parseFloat((performance.now() - reqStart).toFixed(2)) }));
          }
        } else if (parsedMessage.type === 'increment' || parsedMessage.type === 'INCREMENT') {
          const incrStart = performance.now();
          const newValue = await redisClient.incr('global_counter');
          const incrLatency = parseFloat((performance.now() - incrStart).toFixed(2));
          const totalProcessing = parseFloat((performance.now() - reqStart).toFixed(2));
          const userPerforming = parsedMessage.username || username || 'Unknown';
          
          await redisClient.publish('counter_updates', newValue.toString());
          await redisClient.publish('admin_events', JSON.stringify({ 
              type: 'USER_INCREMENT', username: userPerforming, hostname: HOSTNAME, 
              oldValue: newValue - 1, newValue, processingLatency: totalProcessing, redisLatency: incrLatency, recipients: wss.clients.size 
          }));
        } else if (parsedMessage.type === 'OPERATION') {
            console.log("RECEIVED OPERATION:", parsedMessage);
            // Platform & Chaos Operations Handlers
            const { op, payload } = parsedMessage;
            try {
                if (op === 'DELETE_POD') {
                    ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: `Pod ${payload.podName} deleted` }));
                    setTimeout(async () => {
                        try { await k8sApi.deleteNamespacedPod(payload.podName, 'websocket-app', undefined, undefined, 0, undefined, 'Background'); } catch(e){ console.error("Error in DELETE_POD:", e) }
                    }, 500);
                } else if (op === 'RESTART_POD') {
                    ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: `Pod ${payload.podName} restarted` }));
                    setTimeout(async () => {
                        try { await k8sApi.deleteNamespacedPod(payload.podName, 'websocket-app', undefined, undefined, 0, undefined, 'Background'); } catch(e){ console.error("Error in RESTART_POD:", e) }
                    }, 500);
                } else if (op === 'SCALE_APP') {
                    const patch = { spec: { replicas: parseInt(payload.replicas) } };
                    await k8sAppsApi.patchNamespacedDeployment('app-deployment', 'websocket-app', patch, undefined, undefined, undefined, undefined, undefined, { headers: { 'Content-Type': 'application/strategic-merge-patch+json' } });
                    ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: `Scaled app to ${payload.replicas}` }));
                } else if (op === 'STRESS_CPU') {
                    await launchStressPod('cpu');
                    ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: 'Launched CPU stress pod' }));
                } else if (op === 'STRESS_MEM') {
                    await launchStressPod('mem');
                    ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: 'Launched Memory stress pod' }));
                } else if (op === 'STOP_ALL_STRESS') {
                    const stressPods = Array.from(podCache.values()).filter(p => p.metadata.name.startsWith('stress-'));
                    ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: `Stopping ${stressPods.length} stress pods...` }));
                    setTimeout(async () => {
                        for (const pod of stressPods) {
                            try { await k8sApi.deleteNamespacedPod(pod.metadata.name, 'websocket-app'); } catch(e){ console.error("Error in STOP_ALL_STRESS:", e) }
                        }
                    }, 500);
                } else if (op === 'START_POD') {
                    ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: `Starting process in pod ${payload.podName}` }));
                    setTimeout(async () => {
                        try {
                            const exec = new k8s.Exec(kc);
                            const containerName = payload.podName.includes('redis') ? 'redis' : (payload.podName.includes('sentinel') ? 'sentinel' : 'app');
                            exec.exec('websocket-app', payload.podName, containerName, ['/bin/sh', '-c', 'kill -CONT 1'], process.stdout, process.stderr, null, false);
                            
                            const patch = [{ op: 'remove', path: '/metadata/labels/stopped' }];
                            const options = { headers: { 'Content-type': k8s.PatchUtils.PATCH_FORMAT_JSON_PATCH } };
                            await k8sApi.patchNamespacedPod(payload.podName, 'websocket-app', patch, undefined, undefined, undefined, undefined, undefined, options);
                        } catch(e){ console.error("Error in START_POD:", e) }
                    }, 500);
                } else if (op === 'STOP_POD') {
                    ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: `Freezing process in pod ${payload.podName}` }));
                    setTimeout(async () => {
                        try {
                            const exec = new k8s.Exec(kc);
                            const containerName = payload.podName.includes('redis') ? 'redis' : (payload.podName.includes('sentinel') ? 'sentinel' : 'app');
                            exec.exec('websocket-app', payload.podName, containerName, ['/bin/sh', '-c', 'kill -STOP 1'], process.stdout, process.stderr, null, false);
                            
                            let patch = [{ op: 'add', path: '/metadata/labels/stopped', value: 'true' }];
                            if (!podCache.get(payload.podName).metadata.labels) patch = [{ op: 'add', path: '/metadata/labels', value: { stopped: 'true' } }];
                            const options = { headers: { 'Content-type': k8s.PatchUtils.PATCH_FORMAT_JSON_PATCH } };
                            await k8sApi.patchNamespacedPod(payload.podName, 'websocket-app', patch, undefined, undefined, undefined, undefined, undefined, options);
                            
                            // Auto-delete if not started within 5 seconds
                            setTimeout(async () => {
                                const p = podCache.get(payload.podName);
                                if (p && p.metadata && p.metadata.labels && p.metadata.labels.stopped === 'true') {
                                    console.log(`Pod ${payload.podName} still stopped after 5s. Auto-deleting.`);
                                    try {
                                        await k8sApi.deleteNamespacedPod(payload.podName, 'websocket-app', undefined, undefined, 0, undefined, 'Background');
                                    } catch(delErr) { console.error("Error auto-deleting pod:", delErr); }
                                }
                            }, 5000);
                        } catch(e){ console.error("Error in STOP_POD:", e) }
                    }, 500);
                } else if (op === 'DELETE_APP_POD') {
                    const pods = Array.from(podCache.values()).filter(p => p.metadata.name.startsWith('app-deployment'));
                    if (pods.length > 0) {
                        const podToKill = pods[Math.floor(Math.random() * pods.length)];
                        ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: `Deleted pod ${podToKill.metadata.name}` }));
                        setTimeout(async () => {
                            try { await k8sApi.deleteNamespacedPod(podToKill.metadata.name, 'websocket-app', undefined, undefined, 0, undefined, 'Background'); } catch(e){ console.error("Error in DELETE_APP_POD:", e) }
                        }, 500);
                    } else {
                        ws.send(JSON.stringify({ type: 'OP_ERROR', op, error: 'No app pods found to delete' }));
                    }
                } else if (op === 'FAIL_APP_POD') {
                    const pods = Array.from(podCache.values()).filter(p => p.metadata.name.startsWith('app-deployment') && p.status.phase === 'Running');
                    if (pods.length > 0) {
                        const podToFail = pods[Math.floor(Math.random() * pods.length)];
                        ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: `Sent kill signal to ${podToFail.metadata.name}` }));
                        setTimeout(() => {
                            try {
                                const exec = new k8s.Exec(kc);
                                exec.exec('websocket-app', podToFail.metadata.name, 'app', ['/bin/sh', '-c', 'kill 1'], process.stdout, process.stderr, null, false);
                            } catch(e){ console.error("Error in FAIL_APP_POD:", e) }
                        }, 500);
                    } else {
                        ws.send(JSON.stringify({ type: 'OP_ERROR', op, error: 'No running app pods found to fail' }));
                    }
                } else if (op === 'DELETE_REDIS_POD') {
                    if (currentMasterName && currentMasterName !== 'Unknown') {
                        const actualPodName = `${currentMasterName}-0`;
                        ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: `Deleted primary pod ${actualPodName}` }));
                        setTimeout(async () => {
                            try { await k8sApi.deleteNamespacedPod(actualPodName, 'websocket-app', undefined, undefined, 0, undefined, 'Background'); } catch(e){ console.error("Error in DELETE_REDIS_POD:", e) }
                        }, 500);
                    } else {
                        ws.send(JSON.stringify({ type: 'OP_ERROR', op, error: 'Primary pod name is currently unknown' }));
                    }
                } else if (op === 'FAIL_REDIS_POD') {
                    if (currentMasterName && currentMasterName !== 'Unknown') {
                        const actualPodName = `${currentMasterName}-0`;
                        ws.send(JSON.stringify({ type: 'OP_SUCCESS', op, message: `Sent segfault signal to ${actualPodName}` }));
                        setTimeout(() => {
                            try {
                                const exec = new k8s.Exec(kc);
                                exec.exec('websocket-app', actualPodName, 'redis', ['redis-cli', 'debug', 'segfault'], process.stdout, process.stderr, null, false);
                            } catch(e){}
                        }, 500);
                    } else {
                        ws.send(JSON.stringify({ type: 'OP_ERROR', op, error: 'Primary pod name is currently unknown' }));
                    }
                }
            } catch (err) {
                ws.send(JSON.stringify({ type: 'OP_ERROR', op, error: err.message }));
            }
        } else if (parsedMessage.type === 'K8S_EXPLORE') {
            try {
                const { resource, name, ns } = parsedMessage;
                let data = null;
                if (resource === 'nodes') data = await k8sApi.listNode();
                else if (resource === 'namespaces') data = await k8sApi.listNamespace();
                else if (resource === 'pods') data = await k8sApi.listNamespacedPod(ns || 'websocket-app');
                else if (resource === 'services') data = await k8sApi.listNamespacedService(ns || 'websocket-app');
                else if (resource === 'deployments') data = await k8sAppsApi.listNamespacedDeployment(ns || 'websocket-app');
                else if (resource === 'statefulsets') data = await k8sAppsApi.listNamespacedStatefulSet(ns || 'websocket-app');
                else if (resource === 'pod-logs') {
                    const logs = await k8sApi.readNamespacedPodLog(name, ns || 'websocket-app', undefined, undefined, undefined, undefined, undefined, undefined, undefined, 200);
                    data = { logs: logs.body };
                }
                
                let body = data;
                if (data && data.body) body = data.body;
                
                ws.send(JSON.stringify({ type: 'K8S_EXPLORE_RESULT', resource, data: body }));
            } catch (err) {
                ws.send(JSON.stringify({ type: 'OP_ERROR', op: 'K8S_EXPLORE', error: err.message }));
            }
        }
      } catch (err) {
        logEvent('error', 'application_error', { message: err.message, stack: err.stack, request_id: requestId });
      }
    });

    ws.on('close', async () => {
      if (connectionId && username) {
        try {
          await redisClient.zrem('heartbeats', `${connectionId}|${username}|${HOSTNAME}`);
          await removeConnection(connectionId, username, HOSTNAME);
        } catch (err) {}
      }
    });
  });

  server.listen(PORT, () => {
    logEvent('info', 'server_started', { msg: `Server running on port ${PORT}` });
  });
}

startServer();
