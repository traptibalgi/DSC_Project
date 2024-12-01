# Use the existing DEMUCS image as a base
FROM xserrat/facebook-demucs:latest

# Install necessary dependencies
RUN pip install redis minio requests

# Set environment variables 
ENV REDIS_HOST=redis
ENV MINIO_HOST=minio
ENV MINIO_ACCESS_KEY=rootuser
ENV MINIO_SECRET_KEY=rootpass123

# Add the worker-server.py to the container
COPY worker-server.py /app/worker-server.py

# Set the working directory
WORKDIR /app

# Run the worker script as an ENTRYPOINT
ENTRYPOINT ["python3", "worker-server.py"]
