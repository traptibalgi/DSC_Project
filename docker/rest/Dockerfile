FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy files
COPY rest.py /app/
COPY requirements.txt /app/

RUN apt-get update && apt-get install -y redis-tools

# Install dependencies
RUN pip install -r requirements.txt

# Expose the Flask application port
EXPOSE 5000

# Command to run Flask server
CMD ["python", "rest.py"]
