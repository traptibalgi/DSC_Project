#!/bin/bash

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

# Start port forwarding
kubectl port-forward --address 0.0.0.0 service/redis 6379:6379 &
kubectl port-forward -n minio-ns --address 0.0.0.0 service/minio-proj 9000:9000 &
kubectl port-forward -n minio-ns --address 0.0.0.0 service/minio-proj 9001:9001 &
kubectl port-forward --address 0.0.0.0 svc/rest 5000:5000 &

# Notify user
echo "Port forwarding started."
