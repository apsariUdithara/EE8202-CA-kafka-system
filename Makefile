# Convenience targets. Every one of them is a plain command documented in the
# README, so make is optional (it is not installed by default on Windows).

.DEFAULT_GOAL := help
PY ?= python

.PHONY: help install up down reset topics producer consumer dlq test lint format typecheck check demo clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Create a virtualenv and install the project with dev extras
	$(PY) -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -e ".[dev]"

up: ## Start Kafka, Schema Registry, topics and the web UI
	docker compose up -d
	docker compose ps

down: ## Stop the stack (keeps the Kafka data volume)
	docker compose down

reset: ## Stop the stack and delete all Kafka data
	docker compose down -v
	docker compose up -d

topics: ## List the topics on the broker
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

producer: ## Run the order producer
	order-producer --rate 2

consumer: ## Run the order consumer
	order-consumer

dlq: ## Print the dead letter queue
	order-dlq

test: ## Run the unit tests
	pytest -q

lint: ## Lint with ruff
	ruff check .

format: ## Format with ruff
	ruff format .

typecheck: ## Type-check with mypy
	mypy

check: lint typecheck test ## Everything CI runs

demo: up ## Bring up the stack and print the demo script
	@echo
	@echo "Stack is up. Follow docs/DEMO.md:"
	@echo "  terminal A: order-consumer"
	@echo "  terminal B: order-producer --rate 3"
	@echo "  terminal C: order-dlq"
	@echo "  browser   : http://localhost:8080"

clean: ## Remove build and tooling artefacts
	rm -rf build dist .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
