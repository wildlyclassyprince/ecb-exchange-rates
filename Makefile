.PHONY: help pipeline install

test: ## Run test command
	@echo "this is a test"

help: ## Display this help message
	@echo 'Usage:'
	@echo 'make install'
	@echo 'make pipeline'
	@echo ''
	@echo 'Targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort  | awk 'BEGIN {FS = ":.*?##"}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install package dependencies in a virtual environment
	pip install -U pip -r requirements.txt

pipeline: ## Run the exchange rates pipeline that uses ECB rates
	python src/exchange_rates_pipeline.py

.DEFAULT_GOAL := help
