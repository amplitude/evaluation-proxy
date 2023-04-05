build:
	./graldew assemble

run: build
	./gradlew run --console=plain

docker-build: build
	docker build -t experiment-local-proxy .

docker-run: docker-build
	docker run \
		-e AMPLITUDE_API_KEY=${AMPLITUDE_API_KEY} \
		-e AMPLITUDE_SECRET_KEY=${AMPLITUDE_SECRET_KEY} \
		-e AMPLITUDE_DEPLOYMENT_KEY=${AMPLITUDE_DEPLOYMENT_KEY} \
		-e AMPLITUDE_REDIS_URL=${AMPLITUDE_REDIS_URL} \
		-p 3546:3546 experiment-local-proxy
