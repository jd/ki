check:
	@echo "Running tests…"
	@for i in t/*.py; do PYTHONPATH=. $$i; done
