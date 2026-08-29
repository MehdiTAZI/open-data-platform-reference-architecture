SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

.PHONY: help doctor validate local-up local-down local-status local-secrets smoke-test spark-smoke-test

help:
	@echo "Open Data Platform Reference Architecture"
	@echo ""
	@echo "Targets:"
	@echo "  doctor            Check required developer tools"
	@echo "  validate          Run repository static validations"
	@echo "  local-up          Create the full standalone Kind platform"
	@echo "  local-down        Delete the standalone platform and local credentials"
	@echo "  local-status      Show control/data/observability plane resources"
	@echo "  local-secrets     Ensure standalone-only credentials exist"
	@echo "  smoke-test        Execute end-to-end platform health checks"
	@echo "  spark-smoke-test  Submit SparkPi using Spark-on-Kubernetes"

doctor:
	@./scripts/doctor.sh

validate:
	@./scripts/validate.sh

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
