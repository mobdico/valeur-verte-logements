.PHONY: help up down logs shell-etl shell-notebook shell-dashboard clean

# Variables
COMPOSE_FILE = docker-compose.yml
ENV_FILE = .env

# Aide
help:
	@echo "Commandes disponibles:"
	@echo "  make up              - Démarrer tous les services"
	@echo "  make down            - Arrêter tous les services"
	@echo "  make logs            - Afficher les logs de tous les services"
	@echo "  make shell-etl       - Accéder au shell du service ETL"
	@echo "  make shell-notebook  - Accéder au shell du service Notebook"
	@echo "  make shell-dashboard - Accéder au shell du service Dashboard"
	@echo "  make clean           - Nettoyer les volumes et images"
	@echo "  make status          - Afficher le statut des services"

# Démarrer tous les services
up:
	@echo "🚀 Démarrage du cluster DataLake..."
	@if [ ! -f $(ENV_FILE) ]; then \
		echo "⚠️  Fichier .env non trouvé. Copie depuis env.example..."; \
		cp env.example .env; \
	fi
	docker-compose -f $(COMPOSE_FILE) up -d
	@echo "✅ Cluster démarré !"
	@echo "📊 MinIO Console: http://localhost:9001 (admin/password123)"
	@echo "📓 JupyterLab: http://localhost:8888"
	@echo "📈 Dashboard: http://localhost:8501"

# Arrêter tous les services
down:
	@echo "🛑 Arrêt du cluster DataLake..."
	docker-compose -f $(COMPOSE_FILE) down
	@echo "✅ Cluster arrêté !"

# Afficher les logs
logs:
	@echo "📋 Affichage des logs..."
	docker-compose -f $(COMPOSE_FILE) logs -f

# Accéder au shell du service ETL
shell-etl:
	@echo "🐍 Accès au shell du service ETL..."
	docker-compose -f $(COMPOSE_FILE) exec etl bash

# Accéder au shell du service Notebook
shell-notebook:
	@echo "📓 Accès au shell du service Notebook..."
	docker-compose -f $(COMPOSE_FILE) exec notebook bash

# Accéder au shell du service Dashboard
shell-dashboard:
	@echo "📈 Accès au shell du service Dashboard..."
	docker-compose -f $(COMPOSE_FILE) exec dashboard bash

# Afficher le statut des services
status:
	@echo "📊 Statut des services:"
	docker-compose -f $(COMPOSE_FILE) ps

# Nettoyer les volumes et images
clean:
	@echo "🧹 Nettoyage des volumes et images..."
	docker-compose -f $(COMPOSE_FILE) down -v --rmi all
	docker system prune -f
	@echo "✅ Nettoyage terminé !"

# Vérifier la santé des services
health:
	@echo "🏥 Vérification de la santé des services..."
	@echo "MinIO: $(shell curl -s -o /dev/null -w "%{http_code}" http://localhost:9000/minio/health/live || echo "DOWN")"
	@echo "Notebook: $(shell curl -s -o /dev/null -w "%{http_code}" http://localhost:8888 || echo "DOWN")"
	@echo "Dashboard: $(shell curl -s -o /dev/null -w "%{http_code}" http://localhost:8501 || echo "DOWN")"
