# Drew's Video + Socials Scheduler — top-level command dispatcher.
#
# Every operation has one place to live: this file. Targets are thin wrappers
# around the underlying scripts (run.sh, scripts/deploy-cloudflare.sh,
# macos/build.sh, macos/debug.sh) so their internal SCRIPT_DIR logic still
# works. Run `make` (or `make help`) for the summary.

.DEFAULT_GOAL := help
.PHONY: help run deploy deploy-dry build release debug clean

help: ## Show this help
	@printf 'Usage: make <target> [ARGS="..."]\n\n'
	@awk -F':.*## ' '/^[a-zA-Z_-]+:.*## / { printf "  %-12s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

run: ## Start the dev server (creates/updates venv; pass flags via ARGS="--reload")
	@./run.sh $(ARGS)

deploy: ## Sync cloudflare/ callbacks to the site repo, commit, push
	@./scripts/deploy-cloudflare.sh

deploy-dry: ## Show what `make deploy` would change without writing or pushing
	@./scripts/deploy-cloudflare.sh --dry-run

build: ## Build the macOS .app (debug — fast, local; requires a Developer ID Application cert)
	@./macos/build.sh --debug

release: ## Build + sign + notarize + DMG (release)
	@./macos/build.sh --release

debug: ## Dump diagnostics on the built .app
	@./macos/debug.sh

clean: ## Remove build/ and macos/build/ artifacts
	@if [ -d build ]; then rm -r build && echo "removed build/"; fi
	@if [ -d macos/build ]; then rm -r "macos/build" && echo "removed macos/build/"; fi
	@echo "clean: done"
