# Evaluation Proxy

[![docker image version](https://img.shields.io/docker/v/amplitudeinc/evaluation-proxy?color=blue&label=docker&logo=docker&logoColor=white)](https://hub.docker.com/r/amplitudeinc/evaluation-proxy)

Service to optimize local evaluation running within your infrastructure, or support evaluation on unsupported platforms and programming languages.

 * Preload & manage flag configurations and targeted cohorts for local evaluation SDKs running in proxy mode.
 * Expose local evaluation apis for SDKs and API endpoints to enable unsupported platforms and programming languages.
 * Automatically tracks assignments events for local evaluations run on the service.

See the [full documentation on in the Amplitude developer docs](https://docs.developers.amplitude.com/experiment/infra/evaluation-proxy/).

## Configuration

The evaluation proxy is either configured via a `yaml` file (recommended, more configuration options), or using environment variables.

The default location for the configuration yaml file is `/etc/evaluation-proxy-config.yaml`. You may also configure the file location using the `PROXY_CONFIG_FILE_PATH` environment variable.

```yaml
projects:
  - apiKey: "YOUR API KEY"
    secretKey: "YOUR SECRET KEY"
    managementKey: "YOUR MANAGEMENT API KEY"

configuration:
  redis:
    uri: "YOUR REDIS URI" # e.g. "redis://localhost:6379"
```

The developer docs contain [additional information about configuration](https://docs.developers.amplitude.com/experiment/infra/evaluation-proxy#configuration).

## Deployment

The evaluation proxy is stateless, and should be deployed with multiple instances behind a load balancer for high availability and scalability.
For example, a kubernetes deployment with greater than one replica.

### Kubernetes

Use the evaluation proxy [Helm chart](https://github.com/amplitude/evaluation-proxy-helm) to install the proxy service on kubernetes or generate the files needed to deploy the service manually.

### Docker Compose Example

Run the container locally with redis persistence using `docker compose`. You must first update the `compose-config.yaml` file with your project keys before running the composition.

```
docker compose build
docker compose up
```
