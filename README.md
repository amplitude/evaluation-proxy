# Evaluation Proxy

[![docker image version](https://img.shields.io/docker/v/amplitudeinc/evaluation-proxy?color=blue&label=docker&logo=docker&logoColor=white)](https://hub.docker.com/r/amplitudeinc/evaluation-proxy)

Service to optimize local evaluation running within your infrastructure, or support evaluation on unsupported platforms and programming languages.

 * Preload & manage flag configurations and targeted cohorts for local evaluation SDKs running in proxy mode.
 * Expose local evaluation apis for SDKs and API endpoints to enable unsupported platforms and programming languages.
 * Automatically tracks assignments events for local evaluations run on the service.

See the [full documentation on in the Amplitude developer docs](https://docs.developers.amplitude.com/experiment/infra/evaluation-proxy/).

## Deployment

The evaluation proxy is stateless, and should be deployed with multiple instances behind a load balancer for high availability and scalability.
For example, a kubernetes deployment with greater than one replica.

### Docker Compose Example

Service is generally deployed via a [docker image](https://hub.docker.com/r/amplitudeinc/evaluation-proxy).

Run the container locally with redis persistence using `docker compose`. You must first update the `compose-config.yaml` file with your project and deployment keys before running the composition.

```
docker compose up
```


