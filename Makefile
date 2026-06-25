.PHONY: deploy test

deploy:
	docker compose --profile deploy up

test:
	docker compose --profile tests run --rm --build tests
