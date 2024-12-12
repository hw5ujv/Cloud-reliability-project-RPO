FROM ubuntu:20.04

# Update and install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    python3 \
    python3-pip

# Install MinIO server binary
RUN wget https://dl.min.io/server/minio/release/linux-amd64/minio -O /usr/local/bin/minio && \
    chmod +x /usr/local/bin/minio

# Install MinIO client binary (optional)
RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

# Install Python MinIO SDK
RUN pip3 install minio

# Set working directory
WORKDIR /app
