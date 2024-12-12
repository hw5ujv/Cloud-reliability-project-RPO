# Cloud Storage Reliability Experiments

This repository provides scripts and configurations to simulate and test various data recovery and failover strategies in a cloud storage environment using MinIO containers.

## Overview

The setup includes:
- **MinIO Region 1:** Primary region for baseline operations.
- **MinIO Region 2:** Secondary region for testing geo-redundancy and failover scenarios.
- **MinIO RAID Setup:** A MinIO instance configured with multiple data directories to simulate RAID-like resilience against drive failures.

The experiments focus on measuring:
- **Recovery Time Objective (RTO):** How quickly data becomes accessible after a failure.
- **Recovery Point Objective (RPO):** How much data is successfully restored after a failure.

## Prerequisites

- Docker and Docker Compose installed.
- Python 3.x environment.
- MinIO Python client library (install via `pip install minio` or `pip install -r requirements.txt` if available).

## Directory Structure

- **Dockerfile:** Builds an Ubuntu-based MinIO image.
- **docker-compose.yml:** Defines the multi-container setup.
- **scripts/geo_redandancy_advanced.py:** Tests geo-redundant replication and failover.
- **scripts/automated_backup_advanced.py:** Tests automated backup and restore scenarios.
- **data/, data1/, data2/, etc.:** Volumes for MinIO servers.

## Getting Started

1. **Build and Start the Environment:**
   ```bash
   docker-compose build
   docker-compose up -d
