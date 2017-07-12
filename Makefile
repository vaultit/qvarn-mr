.PHONY: install
install:
	pip install -f vendor -r requirements-dev.txt -e . 

.PHONY: help
help:
	@echo "For all targets we assume that python virtualenv is active."
	@echo ""
	@echo "make                      # build everything (for dev environment)"
	@echo "make test                 # run tests"
	@echo "make requirements         # update requirements*.txt from requirements/*.in"
	@echo "make update-requirements  # upgrade and requirements*.txt from requirements/*.in"


.PHONY: test
test:
	py.test --cov-report=term-missing --cov=qvarnmr tests


.PHONY: requirements
requirements:
	pip-compile -f vendor/ requirements/prod.in -o requirements.txt
	pip-compile -f vendor/ requirements/prod.in requirements/dev.in -o requirements-dev.txt


.PHONY: update-requirements
update-requirements: environ
	pip install -U pip setuptools wheel
	pip install -U -r requirements/prod.in
	pip install -U -r requirements/dev.in
	pip-compile -f vendor/ requirements/prod.in -o requirements.txt
	pip-compile -f vendor/ requirements/prod.in requirements/dev.in -o requirements-dev.txt
