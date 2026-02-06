# Bus Alert System - Kubernetes Production Deployment

This guide explains how to set up the production AI service pipeline. This configuration is optimized for **CPU-based scaling** (multiple instances).

## 🚀 Architecture Overview

1. **Proxy (Nginx)**: Entry point and load balancer.
2. **AI Service**: Scaled to multiple replicas to utilize CPU multi-threading.
3. **MinIO**: Shared storage for images and results.
4. **Workers**: Consumers that process jobs from MinIO.

---

## 💻 Local / Origin Machine Commands
Run these on your local development machine to prepare the system.

### 1. Build Docker Image
```bash
docker build -t bus-alert-system:latest .
```

### 2. Push Image to Registry
If your external server cannot access your local images, push to a registry (Docker Hub, GitHub Registry, etc.):
```bash
docker tag bus-alert-system:latest your-repo/bus-alert-system:latest
docker push your-repo/bus-alert-system:latest
```

### 3. Prepare Data (Optional)
```bash
python populate_minio.py
```

---

## 🌐 External Server (Kubernetes) Commands
Run these on your GPU/CPU server where Kubernetes is running.

### 1. Deploy the Stack
Apply all manifests in the `k8s/` directory:
```bash
kubectl apply -f k8s/configmap-nginx.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/ai-deployment.yaml
kubectl apply -f k8s/ai-service.yaml
kubectl apply -f k8s/gpu-proxy.yaml
kubectl apply -f k8s/upload-api.yaml
kubectl apply -f k8s/consumer.yaml
```

### 2. Scaling the AI Service
To increase throughput, scale the AI service pods:
```bash
kubectl scale deployment ai-service --replicas=8
```

### 3. Monitoring
- **Check Pods**: `kubectl get pods`
- **Follow Logs**: `kubectl logs -l app=ai-service -f`
- **Service Status**: `kubectl get svc`

---

## 🔍 Troubleshooting
- **Pod Crash**: Check logs with `kubectl logs <pod-name>` or describe with `kubectl describe pod <pod-name>`.
- **503 Errors**: This means the proxy is hits its concurrency limit (currently 20). You can increase this in `k8s/configmap-nginx.yaml`.
