.PHONY: env
env: env/.done 


.PHONY: help
help:
	@echo "For all targets we assume that python virtualenv is active."
	@echo ""
	@echo "make                      # build everything (for dev environment)"
	@echo "make test                 # run tests"
	@echo "make requirements         # update requirements*.txt from requirements/*.in"
	@echo "make update-requirements  # upgrade and requirements*.txt from requirements/*.in"


.PHONY: test
test: env
	env/bin/py.test --cov-report=term-missing --cov=qvarnmr tests


.PHONY: requirements
requirements:
	env/bin/pip-compile -f vendor/ requirements/prod.in -o requirements.txt
	env/bin/pip-compile -f vendor/ requirements/prod.in requirements/dev.in -o requirements-dev.txt


.PHONY: update-requirements
update-requirements: env
	env/bin/pip install -U pip setuptools wheel
	env/bin/pip install -U -r requirements/prod.in
	env/bin/pip install -U -r requirements/dev.in
	env/bin/pip-compile -f vendor/ requirements/prod.in -o requirements.txt
	env/bin/pip-compile -f vendor/ requirements/prod.in requirements/dev.in -o requirements-dev.txt


env/bin/pip:
	virtualenv -p python3.4 env
	env/bin/pip install -U pip setuptools wheel
	env/bin/pip install -I setuptools   # workaround for https://github.com/pypa/setuptools/issues/887

env/.done: env/bin/pip requirements-dev.txt setup.py
	env/bin/pip install -f vendor/ -r requirements-dev.txt -e .
	touch env/.done
