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
		-v `pwd`/config.yaml:/etc/evaluation-proxy-config.yaml \
		-p 3546:3546 experiment-local-proxy

docker-compose:
	docker compose up

