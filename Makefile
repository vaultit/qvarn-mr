.PHONY: install
install:
	pip install -f vendor -r requirements.txt -e . 

.PHONY: test
test:
	py.test --cov-report=term-missing --cov=qvarnmr tests
