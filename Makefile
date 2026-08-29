SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

.PHONY: help doctor validate local-up local-down smoke-test

help:
	@echo "Open Data Platform Reference Architecture"
	@echo ""
	@echo "Targets:"
	@echo "  doctor       Check required developer tools"
	@echo "  validate     Run repository static validations"
	@echo "  local-up     Create the standalone Kind control plane"
	@echo "  local-down   Delete the standalone Kind control plane"
	@echo "  smoke-test   Validate Kubernetes control-plane health"

doctor:
	@./scripts/doctor.sh

validate:
	@./scripts/validate.sh

local-up: doctor
	@./scripts/local-up.sh

local-down:
	@./scripts/local-down.sh

smoke-test:
	@./scripts/smoke-test.sh
