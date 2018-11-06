.PHONY: run test

run:
	docker-compose up --build


test: run
	docker-compose run beehive-service /app/node_modules/.bin/mocha src/**/*.spec.js",
