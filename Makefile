.PHONY: deploy test

deploy:
	docker compose --profile deploy up --build

build:
	docker compose build api worker
	docker image prune -f

test:
	docker compose --profile tests run --rm --build tests
