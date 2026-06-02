# Deployment & Installation Guide

This guide provides step-by-step instructions to deploy, configure, and run the **datatouille** platform in your local environment.

---

## 📋 Prerequisites

Ensure your system meets the following requirements before starting:

### System Resources
* **RAM**: 8 GB minimum (16 GB recommended)
* **CPU**: 4 cores minimum
* **Storage**: 20 GB free space (SSD recommended)

### Required Software
* **Docker Desktop**: v4.x or higher
* **Python**: v3.12 or higher
* **Git**: Latest stable release

Verify your environment by running:
```bash
docker --version
docker compose version
python3 --version
git --version
```

---

## 🚀 Setup & Installation

### 1. Clone the Repository
Clone the project and navigate to the root directory:
```bash
git clone <repository-url> datatouille && cd datatouille
```

### 2. Configure Environment Variables
Copy the template configuration file to create your active environment file:
```bash
cp .env.example .env
```
Open the `.env` file and generate secure values for `AIRFLOW__CORE__FERNET_KEY` and `AIRFLOW__API_AUTH__JWT_SECRET` as described in the comments.

### 3. Spin Up the Docker Stack
Launch the 12-service container stack in detached mode:
```bash
docker compose up -d
```

### 4. Verify Service Health
Check that all containers are healthy and running:
```bash
docker compose ps
```
*Note: Wait a couple of minutes for all databases and the Airflow scheduler to complete their initialization routines.*

---

## 🌐 Service Access & Credentials

Once the stack is successfully deployed, the following service interfaces are available on localhost:

| Service | Port / URL | Credentials (User / Password) | Description |
|---------|------------|-------------------------------|-------------|
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` | DAG control and orchestration center |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` | Object storage control panel |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` | Dashboards and observability panels |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | *(None)* | Time-series metrics database UI |
| **Flower** | [http://localhost:5555](http://localhost:5555) | *(None)* | Celery task worker monitoring |

---

## 🛠️ Post-Installation Setup

To provision your running Airflow instance with default variables, connection parameters, and configurations, execute:
```bash
make seed-variables
```
This ensures your platform-wide configurations, limits, and API parameters are instantly available inside the Airflow UI (under *Admin > Variables*).
