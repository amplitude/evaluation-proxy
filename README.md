# Evaluation Proxy

Service to optimize local evaluation running within your infrastructure, or support evaluation on unsupported platforms and programming languages.

 * Preload & manage flag configurations and targeted cohorts for local evaluation SDKs running in proxy mode.
 * Expose local evaluation apis for SDKs and API endpoints to enable unsupported platforms and programming languages.
 * Automatically tracks assignments events for local evaluations run on the service.

## Configuration

The evaluation proxy requires keys as environment variables to run. Otherwise, the service will crash on startup.

| Environment Variable     | Description                                                                                                        |
|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `AMPLITUDE_API_KEY`        | The project's api key.                                                                                             |
| `AMPLITUDE_SECRET_KEY`     | The project's secret key.                                                                                          |
| `AMPLITUDE_DEPLOYMENT_KEY` | The key for the deployment to manage. Deployment key must exist within the same project as the api and secret key. |

## Deployment

The evaluation proxy is stateless, and should be deployed with multiple instances behind a load balancer for high availability and scalability.
For example, a kubernetes deployment with greater than one replica

## Docker

Service is deployed via a [docker image](https://hub.docker.com/r/amplitudeinc/evaluation-proxy).

### Pull

```
docker pull amplitudeinc/evaluation-proxy
```

### Run

```
docker run \
    -e AMPLITUDE_API_KEY=${AMPLITUDE_API_KEY} \
    -e AMPLITUDE_SECRET_KEY=${AMPLITUDE_SECRET_KEY} \
    -e AMPLITUDE_DEPLOYMENT_KEY=${AMPLITUDE_DEPLOYMENT_KEY} \
    -p 3546:3546 \
    amplitudeinc/evaluation-proxy
```

## Source

Build and run the service from source.

### Build
```
./gradlew assemble
```

### Run
```
./gradlew run
```


