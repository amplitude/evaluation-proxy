build:
	./graldew assemble

lint:
	./gradlew ktlintFormat

run: build
	AMPLITUDE_LOG_LEVEL=DEBUG PROXY_CONFIG_FILE_PATH=`pwd`/config.yaml PROXY_PROJECTS_FILE_PATH=`pwd`/projects.yaml ./gradlew run --console=plain

docker-build: build
	docker build -t evaluation-proxy:local .

docker-run: docker-build
	docker run \
		-e PROXY_CONFIG_FILE_PATH=/etc/evaluation-proxy-config.yaml \
		-e PROXY_PROJECTS_FILE_PATH=/etc/evaluation-proxy-projects.yaml \
		-v `pwd`/config.yaml:/etc/evaluation-proxy-config.yaml \
		-v `pwd`/projects.yaml:/etc/evaluation-proxy-projects.yaml \
		-p 3546:3546 evaluation-proxy:local
