#!/bin/sh
#
# You can use this script to launch Redis and minio on Kubernetes
# and forward their connections to your local computer. That means
# you can then work on your worker-server.py and rest.py
# on your local computer rather than pushing to Kubernetes with each change.
#
# To kill the port-forward processes us e.g. "ps augxww | grep port-forward"
# to identify the processes ids
#

# Ports to check
PORTS=(6379 9000 9001 5000)

# Function to find and kill processes using specific ports on Windows using Git Bash
kill_processes_on_ports_windows() {
  for PORT in "${PORTS[@]}"; do
    PID=$(netstat -ano | grep :$PORT | awk '{print $5}')
    if [ -n "$PID" ]; then
      echo "Killing process $PID on port $PORT..."
      winpty taskkill //PID $PID //F
    else
      echo "No process found using port $PORT."
    fi
  done
}

# Kill processes using the specified ports
kill_processes_on_ports_windows

kubectl apply -f redis/redis-deployment.yaml
kubectl apply -f redis/redis-service.yaml

kubectl apply -f rest/rest-deployment.yaml
kubectl apply -f rest/rest-service.yaml
kubectl apply -f logs/logs-deployment.yaml
kubectl apply -f worker/worker-deployment.yaml
kubectl apply -f minio/minio-external-service.yaml


kubectl port-forward --address 0.0.0.0 service/redis 6379:6379 &

# If you're using minio from the kubernetes tutorial this will forward those
kubectl port-forward -n minio-ns --address 0.0.0.0 service/minio-proj 9000:9000 &
kubectl port-forward -n minio-ns --address 0.0.0.0 service/minio-proj 9001:9001 &

kubectl port-forward  --address 0.0.0.0 svc/rest 5000:5000 &