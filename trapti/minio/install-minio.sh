#!/bin/sh

# Define the namespace and release name
NAMESPACE="minio-ns"
RELEASE_NAME="minio-proj"

# Step 1: Uninstall any existing MinIO deployment
echo "Uninstalling existing MinIO deployment..."
helm uninstall $RELEASE_NAME -n $NAMESPACE

# Step 2: Remove the Bitnami repository (optional step)
echo "Removing Bitnami repository..."
helm repo remove bitnami

# Step 3: Add the Bitnami repository and update
echo "Adding Bitnami repository..."
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# Step 4: Install MinIO with the specified configuration
echo "Installing MinIO..."
helm install -f ./minio-config.yaml -n $NAMESPACE --create-namespace $RELEASE_NAME bitnami/minio

# Step 5: Wait for the service to start
echo "Waiting for MinIO to start..."
sleep 10

# Step 6: Port-forward to access MinIO locally
echo "Starting port-forwarding for MinIO..."
kubectl port-forward --namespace $NAMESPACE svc/$RELEASE_NAME 9000:9000 &
kubectl port-forward --namespace $NAMESPACE svc/$RELEASE_NAME 9001:9001 &

echo "MinIO installation and port-forwarding completed."
