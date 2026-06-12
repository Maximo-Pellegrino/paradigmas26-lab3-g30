JAVA_HOME := /usr/lib/jvm/java-17-openjdk-amd64
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)
export SBT_OPTS := --add-exports=java.base/sun.nio.ch=ALL-UNNAMED

SUBSCRIPTION_FILE ?= data/local_subscriptions.json
ENTITIES_DIR      ?= data/valid_entities
TOP_K             ?= 15

REDDIT_MOCK_DIR := reddit-mock

.PHONY: run stop

run:
	@echo "Levantando servidor mock..."
	cd $(REDDIT_MOCK_DIR) && sbt run &
	@echo "Esperando que el servidor esté listo..."
	sleep 10
	@echo "Corriendo la app principal..."
	sbt "run --subscription-file $(SUBSCRIPTION_FILE) --entities-dir $(ENTITIES_DIR) --top-k $(TOP_K)"

stop:
	@pkill -f "reddit-mock" || true