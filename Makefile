.PHONY: run test

run:
	@sh scripts/data-dir.sh
	@docker-compose up --build


test:
	@docker-compose run beehive-service /app/node_modules/.bin/mocha test/**/*.spec.js
