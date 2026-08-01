import ws from 'k6/ws';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

// Custom metrics to track our specific goals
const successfulConnections = new Counter('ws_successful_connections');
const failedConnections = new Counter('ws_failed_connections');
const messagesSent = new Counter('ws_messages_sent');
const messagesReceived = new Counter('ws_messages_received');

export const options = {
  vus: 1000,
  duration: '60s',
};

export default function () {
  // Use WS_URL environment variable if provided, otherwise default to localhost
  const url = __ENV.WS_URL || 'ws://localhost:3000/';
  
  // Use the k6 VU ID to generate a unique username
  const vuId = exec.vu.idInTest;
  const username = `User ${vuId}`;

  const res = ws.connect(url, null, function (socket) {
    socket.on('open', function () {
      successfulConnections.add(1);

      // Send join event with username
      socket.send(JSON.stringify({ type: 'join', username: username }));
      messagesSent.add(1);

      // Send an increment message every exactly 1 second
      socket.setInterval(function timeout() {
        socket.send(JSON.stringify({ type: 'increment', username: username }));
        messagesSent.add(1);
      }, 1000);
    });

    socket.on('message', function (msg) {
      messagesReceived.add(1);
    });

    socket.on('error', function (e) {
      if (e.error() != 'websocket: close sent') {
        failedConnections.add(1);
        console.error('WebSocket error: ', e.error());
      }
    });

    // Ensure the connection doesn't outlive the test
    socket.setTimeout(function () {
      socket.close();
    }, 60000);
  });

  check(res, { 'connected successfully': (r) => r && r.status === 101 });

  if (!res || res.status !== 101) {
    failedConnections.add(1);
  }
}
