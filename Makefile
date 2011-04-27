check:
	@echo "Running tests…"
	@for i in t/*.py; do echo "Running `basename $$i .py`"; PYTHONPATH=. $$i || exit 1; done
