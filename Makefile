build:
	./graldew assemble

lint:
	./gradlew ktlintFormat

run: build
	PROXY_CONFIG_FILE_PATH=`pwd`/config.yaml ./gradlew run --console=plain

docker-build: build
	docker build -t experiment-local-proxy .

docker-run: docker-build
	docker run \
		-e AMPLITUDE_PROJECT_ID=${AMPLITUDE_PROJECT_ID} \
		-e AMPLITUDE_API_KEY=${AMPLITUDE_API_KEY} \
		-e AMPLITUDE_SECRET_KEY=${AMPLITUDE_SECRET_KEY} \
		-e AMPLITUDE_EXPERIMENT_DEPLOYMENT_KEY=${AMPLITUDE_EXPERIMENT_DEPLOYMENT_KEY} \
		-e AMPLITUDE_REDIS_URL=${AMPLITUDE_REDIS_URL} \
		-e AMPLITUDE_REDIS_PREFIX=${AMPLITUDE_REDIS_PREFIX} \
		-e AMPLITUDE_LOG_LEVEL=${AMPLITUDE_LOG_LEVEL} \
		-p 3546:3546 experiment-local-proxy

docker-compose:
	docker compose up

