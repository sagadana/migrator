
REGION=ap-southeast-2
ACCOUNT_ID=704221390959

install:
	@echo "Installing dependencies for all scripts"
	@make install-mongo-redis

# ------------------------------
# MONGO TO REDIS (mongo-redis)
# ------------------------------

MONGO_REDIS_IMAGE=db-migrate-mongo-redis
MONGO_REDIS_TAG=latest

install-mongo-redis:
	@echo "Creating .env file if not exists"
	if [ ! -f ./mongo-redis/.env ]; then cp ./mongo-redis/.env.example ./mongo-redis/.env; fi

	@echo "Installing go modules"
	@cd ./mongo-redis && go mod tidy

mongo-redis-dev:
	@cd ./mongo-redis && go run main.go

mongo-redis-docker:
	@docker compose up mongo-redis --build

mongo-redis-push-aws:

	@echo "AWS ECR login"
	@aws ecr get-login-password --region=${REGION} | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

	@echo "Creating AWS ECR repository if not available"
	@if ! aws ecr describe-repositories --region=${REGION} --repository-names ${MONGO_REDIS_IMAGE} > /dev/null 2>&1; then \
		aws ecr create-repository --region=${REGION} --repository-name ${MONGO_REDIS_IMAGE}; \
	fi
	@echo "Creating ECR lifecycle policy"
	@aws ecr put-lifecycle-policy --repository-name ${MONGO_REDIS_IMAGE} --lifecycle-policy-text '{"rules":[{"rulePriority":1,"description":"Expire all but the latest image","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":1},"action":{"type":"expire"}}]}' --region=${REGION}

	@echo "Building docker image"
	@cd ./mongo-redis && docker build -t ${MONGO_REDIS_IMAGE}:${MONGO_REDIS_TAG} .

	@echo "Tagging docker image"
	@docker tag ${MONGO_REDIS_IMAGE}:${MONGO_REDIS_TAG} ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${MONGO_REDIS_IMAGE}:${MONGO_REDIS_TAG}

	@echo "Pushing docker image"
	@docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${MONGO_REDIS_IMAGE}:${MONGO_REDIS_TAG}