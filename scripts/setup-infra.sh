#!/usr/bin/env bash
# scripts/setup-infra.sh
# ─────────────────────────────────────────────────────────────────────────────
# One-shot script to install all production infrastructure components
# into a Docker Desktop Kubernetes cluster via Helm.
# Run once. Then deploy the app via ArgoCD or Helm.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }

# ── Prerequisites check ───────────────────────────────────────────────────────
for cmd in kubectl helm docker; do
  command -v "$cmd" &>/dev/null || { echo "ERROR: $cmd not found. Install it first."; exit 1; }
done

info "Verifying Kubernetes connectivity..."
kubectl cluster-info --request-timeout=5s || { echo "ERROR: Cannot reach cluster. Is Docker Desktop Kubernetes running?"; exit 1; }

# ── Helm repos ────────────────────────────────────────────────────────────────
info "Adding Helm repositories..."
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana             https://grafana.github.io/helm-charts
helm repo add falcosecurity        https://falcosecurity.github.io/charts
helm repo add argo                 https://argoproj.github.io/argo-helm
helm repo update

# ── 1. Metrics Server (for HPA) ──────────────────────────────────────────────
info "Installing metrics-server..."
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl patch deployment metrics-server -n kube-system --type='json' \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' 2>/dev/null || true

# ── 2. NGINX Ingress Controller ───────────────────────────────────────────────
info "Installing nginx-ingress-controller..."
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.8.2/deploy/static/provider/cloud/deploy.yaml

# ── 3. Monitoring namespace ───────────────────────────────────────────────────
info "Creating monitoring namespace..."
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

# ── 4. kube-prometheus-stack (Prometheus + Grafana + Alertmanager) ────────────
info "Installing kube-prometheus-stack..."
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --values monitoring/kube-prometheus-stack-values.yaml \
  --wait --timeout=10m

# ── 5. Loki + Promtail ───────────────────────────────────────────────────────
info "Installing loki-stack (Loki + Promtail)..."
helm upgrade --install loki grafana/loki-stack \
  --namespace monitoring \
  --values monitoring/loki-values.yaml \
  --wait --timeout=5m

# ── 6. Grafana Tempo (Distributed Tracing) ────────────────────────────────────
info "Installing Grafana Tempo..."
helm upgrade --install tempo grafana/tempo \
  --namespace monitoring \
  --values monitoring/tempo-values.yaml \
  --wait --timeout=5m

# ── 7. ArgoCD ────────────────────────────────────────────────────────────────
info "Installing ArgoCD..."
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install argocd argo/argo-cd \
  --namespace argocd \
  --set server.service.type=NodePort \
  --set server.service.nodePortHttp=30080 \
  --wait --timeout=10m

# ── 8. Falco (Runtime Security) ───────────────────────────────────────────────
info "Installing Falco..."
kubectl create namespace falco --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install falco falcosecurity/falco \
  --namespace falco \
  --values monitoring/falco-values.yaml \
  --wait --timeout=10m

# ── 9. Import Grafana Dashboard ───────────────────────────────────────────────
info "Importing Grafana dashboard..."
kubectl create configmap grafana-dashboards \
  --from-file=websocket-dashboard.json=monitoring/grafana-dashboard.json \
  --namespace monitoring \
  --dry-run=client -o yaml | kubectl apply -f -

# ── 10. Apply PrometheusRule & AlertmanagerConfig ─────────────────────────────
info "Applying Prometheus alert rules..."
kubectl apply -f monitoring/prometheus-rules.yaml

# ── 11. Register ArgoCD application ───────────────────────────────────────────
info "Registering ArgoCD application..."
warn "Make sure you have updated argocd/application.yaml with your actual GitHub repo URL!"
kubectl apply -f argocd/application.yaml

# ── Done ──────────────────────────────────────────────────────────────────────
info "────────────────────────────────────────────────────────────────"
info "Infrastructure setup complete! Access points:"
info ""
info "  App (via Ingress):   http://localhost"
info "  Grafana:             http://localhost  (port-forward below)"
info "  ArgoCD:              http://localhost:30080"
info ""
info "  kubectl port-forward svc/kube-prometheus-stack-grafana 3001:80 -n monitoring"
info "  Default Grafana credentials: admin / admin"
info ""
info "  ArgoCD initial admin password:"
info "  kubectl get secret argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 -d"
info "────────────────────────────────────────────────────────────────"
