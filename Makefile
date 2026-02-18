# Makefile

SPARK_JOBS := spark-master spark-worker minio
SPARK_SUBMIT := spark-submit

.PHONY: help up down restart logs ps clean


# Default target
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
	docker-compose build $(SPARK_SUBMIT)
	docker-compose run --rm $(SPARK_SUBMIT)
