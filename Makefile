.PHONY: deploy test

deploy:
	docker compose --profile deploy up --build

deploy-without-worker:
	docker compose --profile deploy-without-worker up --build

build:
	docker compose build api worker
	docker image prune -f

test:
	docker compose --profile tests run --rm --build tests
