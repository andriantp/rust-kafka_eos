FILE    = docker/docker-compose.yml

.PHONY: up down restart ps kafka akhq registry clean system volume network

# ======================== network ========================
network:
	@if ! docker network inspect kafka_network > /dev/null 2>&1; then \
		echo "Network kafka_network not found, creating..."; \
		docker network create --driver=bridge kafka_network; \
	else \
		echo "Network kafka_network already exists."; \
	fi

# ======================== prune ========================  
clean:system volume
	@echo "🧹 Docker cleanup completed."
volume:
	@echo "🧹 Docker volume cleanup completed."
	docker volume prune -a -f 
system: 
	@echo "🧹 Docker system cleanup completed."
	docker system prune -a -f

# ======================== kafka ========================
up:network
	@echo "🐳 Starting (Kafka & AKHQ) without SASL-PLAIN containers ..."
	mkdir  docker/kafka
	chmod -R 777 docker/kafka
	docker compose -f $(FILE) up --force-recreate -d --build 
	@echo "✅ (Kafka & AKHQ) without SASL-PLAIN are up"

down:
	@echo "🛑 Stopping (Kafka + AKHQ) without SASL-PLAIN containers ..."
	docker compose -f $(FILE) down
	@echo "✅ Containers stopped"

restart:
	@echo "🔄 Restarting Kafka stack..."
	docker compose -f $(FILE) down
	docker compose -f $(FILE) up -d
	@echo "✅ Restart complete"

# ======================== log ========================  
ps:
	@echo "📋 Checking container status..."
	docker ps -a --filter "name=kafka"


zookeeper:
	@echo "📜 Showing Kafka logs..."
	docker logs -f kafka-zookeeper

kafka:
	@echo "📜 Showing Kafka logs..."
	docker logs -f kafka-broker

akhq:
	@echo "📜 Showing AKHQ logs..."
	docker logs -f kafka-akhq

registry:
	@echo "📜 Showing AKHQ logs..."
	docker logs -f kafka-schema-registry