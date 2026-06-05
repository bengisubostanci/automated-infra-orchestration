
<img width="1256" height="885" alt="Ekran görüntüsü 2026-06-05 234938" src="https://github.com/user-attachments/assets/537da92a-d9de-479c-9966-dd0be241c401" />


<img width="1716" height="885" alt="Ekran görüntüsü 2026-06-05 235527" src="https://github.com/user-attachments/assets/c55eac0a-5ffb-4382-85d6-da3e1a59a654" />


# Automated Multi-Node Infrastructure & Platform Orchestration

This repository contains the foundational configuration for an automated, multi-node infrastructure designed for scalable data processing, centralized metadata management, and S3-compatible object storage. The entire ecosystem is fully containerized and orchestrated using advanced multi-container environments.

## Architecture Overview

The platform orchestrates the following core data engineering and infrastructure components within a unified private network (`infra_network`):

* **Distributed Processing Layer:** Apache Spark 3.5.0 (Master & Worker architecture) for high-performance big data computation.
* **Storage Engine:** MinIO (S3-Compatible Object Storage) acting as a local cloud data lake for raw and structured analytical assets.
* **Metadata Store:** PostgreSQL 15 (Alpine-based) serving as the centralized relational database for platform metadata and schemas.

## Technical Stack & Infrastructure

* **Container Orchestration:** Docker Compose
* **Operating System/Runtime:** WSL2 (Windows Subsystem for Linux) Core
* **Storage Framework:** Local persistent volumes for database state maintenance (`postgres_data`, `minio_data`).

## Directory Structure


.
├── docker-compose.yml     # Core multi-container service orchestration
├── .env                   # Centralized environment variables & infrastructure configuration
└── .gitignore             # Strict patterns for environment boundaries and local cache prevention

Getting Started
Prerequisites
Docker Desktop with WSL2 backend enabled

Git

Initialization
Clone the repository to your local runtime.

Initialize the orchestration engine to pull and boot the required infrastructure layers:
docker compose up -d

Verification Matrix
Once running, the sandbox endpoints are accessible via the following local loopbacks:

Apache Spark WebUI: http://localhost:8080 (Verify active worker cluster topology)

MinIO Console: http://localhost:9001 (Storage bucket management interface)
