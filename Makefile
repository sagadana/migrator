
STAGING_REGION=ap-southeast-2
STAGING_ACCOUNT_ID=704221390959
SYSTEM_NAME_TAG=seekmax-db-migration


# MONGO TO REDIS (mongo-redis)

MONGO_REDIS_IMAGE=seek/cnc-db-migrate-mongo-redis
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

mongo-redis-stag:

	@echo "AWS ECR login"
	@aws ecr get-login-password --region=${STAGING_REGION} | docker login --username AWS --password-stdin ${STAGING_ACCOUNT_ID}.dkr.ecr.${STAGING_REGION}.amazonaws.com

	@echo "Creating AWS ECR repository if not available"
	@aws ecr describe-repositories --region=${STAGING_REGION} --repository-names ${MONGO_REDIS_IMAGE} || aws ecr create-repository --region=${STAGING_REGION} --repository-name ${MONGO_REDIS_IMAGE} --tags Key=seek:system:name,Value=${SYSTEM_NAME_TAG}
	@echo "Creating ECR lifecycle policy"
	@aws ecr put-lifecycle-policy --repository-name ${MONGO_REDIS_IMAGE} --lifecycle-policy-text '{"rules":[{"rulePriority":1,"description":"Expire all but the latest image","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":1},"action":{"type":"expire"}}]}' --region=${STAGING_REGION}

	@echo "Building docker image"
	@cd ./mongo-redis && docker build -t ${MONGO_REDIS_IMAGE}:${MONGO_REDIS_TAG} .

	@echo "Tagging docker image"
	@docker tag ${MONGO_REDIS_IMAGE}:${MONGO_REDIS_TAG} ${STAGING_ACCOUNT_ID}.dkr.ecr.${STAGING_REGION}.amazonaws.com/${MONGO_REDIS_IMAGE}:${MONGO_REDIS_TAG}

	@echo "Pushing docker image"
	@docker push ${STAGING_ACCOUNT_ID}.dkr.ecr.${STAGING_REGION}.amazonaws.com/${MONGO_REDIS_IMAGE}:${MONGO_REDIS_TAG}