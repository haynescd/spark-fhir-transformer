# 🚀 Local Apache Spark Cluster (Docker + Makefile)

This project provides a simple **local Apache Spark cluster** powered by Docker Compose and controlled via a Makefile.

It spins up:

* Spark Master
* Spark Worker
* Spark Submit container (for running jobs)

---

## 🧱 Architecture

* **Spark Master** – Cluster coordinator
* **Spark Worker** – Executes tasks
* **Spark Submit** – Used to submit jobs to the cluster

You can access the Spark UI at:

```
http://localhost:8080
```

---

## 📦 Prerequisites

* Docker
* Docker Compose
* Make

---

# ⚡ Usage

All commands are wrapped in the Makefile for convenience.

## 🔍 View Available Commands

```bash
make help
```

---

## ▶️ Start Spark Cluster

```bash
make up
```

Starts:

* spark-master
* spark-worker

Runs in detached mode.

---

## 🛑 Stop Spark Cluster

```bash
make down
```

---

## 🔁 Restart Cluster

```bash
make restart
```

---

## 📜 View Logs

```bash
make logs
```

Follows container logs.

---

## 📊 Show Running Containers

```bash
make ps
```

---

## 🧹 Clean Everything (including volumes)

```bash
make clean
```

⚠️ This removes volumes and persisted data.

---

## 🏃 Run a Spark Job

```bash
make run
```

This runs the `spark-submit` container.

Example:

```bash
make run COMMAND="spark-submit --master spark://spark-master:7077 /app/job.py"
```

---

# 🛠 Makefile Reference

```makefile
SPARK_JOBS := spark-master spark-worker
SPARK_SUBMIT := spark-submit

.PHONY: help up down restart logs ps clean

help:
	@echo "Available commands:"
	@echo "  make up        - Start services"
	@echo "  make down      - Stop services"
	@echo "  make restart   - Restart services"
	@echo "  make logs      - Tail logs"
	@echo "  make ps        - Show running containers"
	@echo "  make clean     - Stop and remove volumes"
	@echo "  make run       - Run Spark submit Job"

up:
	docker-compose up $(SPARK_JOBS) -d

down:
	docker-compose down

restart:
	docker-compose down
	docker-compose up $(SPARK_JOBS) -d 

logs:
	docker-compose logs -f

ps:
	docker-compose ps

clean:
	docker-compose down -v

run:
	docker-compose run --rm $(SPARK_SUBMIT)
```

---

