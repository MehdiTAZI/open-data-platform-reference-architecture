SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

.PHONY: help doctor validate build-local-images local-up local-down local-status local-secrets smoke-test spark-smoke-test lakehouse-smoke-test

help:
	@echo "Open Data Platform Reference Architecture"
	@echo ""
	@echo "Targets:"
	@echo "  doctor                Check required developer tools"
	@echo "  validate              Run repository static validations"
	@echo "  build-local-images    Build and load reference runtime images"
	@echo "  local-up              Create the full standalone Kind platform"
	@echo "  local-down            Delete the standalone platform and local credentials"
	@echo "  local-status          Show control/data/observability plane resources"
	@echo "  local-secrets         Ensure standalone-only credentials exist"
	@echo "  smoke-test            Execute full platform health and interoperability checks"
	@echo "  spark-smoke-test      Submit SparkPi using Spark-on-Kubernetes"
	@echo "  lakehouse-smoke-test  Verify Spark write -> Trino read through Polaris/Iceberg"

doctor:
	@./scripts/doctor.sh

validate:
	@./scripts/validate.sh

build-local-images:
	@./scripts/build-local-images.sh

local-up: doctor
	@./scripts/local-up.sh

local-down:
	@./scripts/local-down.sh

local-status:
	@./scripts/local-status.sh

local-secrets:
	@./scripts/local-secrets.sh

smoke-test:
	@./scripts/smoke-test.sh

spark-smoke-test:
	@./scripts/spark-smoke-test.sh

lakehouse-smoke-test:
	@./scripts/lakehouse-smoke-test.sh
