.PHONY: deploy test

deploy:
	docker compose --env-file .env --profile deploy up --build --no-attach mongo

deploy-without-worker:
	docker compose --env-file .env --profile deploy-without-worker up --build

build:
	docker compose build api worker
	docker image prune -f

TEST ?= tests/

test:
	docker compose --profile tests run --rm --build tests pytest $(TEST) --integration -v
