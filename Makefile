mongo-redis:
	# Create .env file if not exists
	if [ ! -f .env ]; then cp .env.example .env; fi

	# Run docker compose
	docker compose up mongo-redis --build


mongo-redis-dev:
	# Create .env file if not exists
	if [ ! -f .env ]; then cp .env.example .env; fi

	# Run go script
	go run mongo-redis.go