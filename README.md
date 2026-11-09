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
    useCluster: false # Set to true for Redis Cluster
    readFrom: "REPLICA_PREFERRED" # Read routing: "REPLICA_PREFERRED" (default) or "ANY"
```

The developer docs contain [additional information about configuration](https://docs.developers.amplitude.com/experiment/infra/evaluation-proxy#configuration).

### EU data residency

Set `serverZone: EU` so that **all** outbound traffic (flag configs, cohorts, the Management API and assignment/exposure events) goes to Amplitude's EU data center. Setting only `serverUrl` and `cohortServerUrl` is not sufficient: the management and analytics endpoints would still default to US.

```yaml
configuration:
  serverZone: "EU"
```

Individual endpoints can still be overridden with `serverUrl`, `cohortServerUrl`, `managementServerUrl` and `analyticsServerUrl` (or the environment variables `AMPLITUDE_SERVER_URL`, `AMPLITUDE_COHORT_SERVER_URL`, `AMPLITUDE_MANAGEMENT_SERVER_URL`, `AMPLITUDE_ANALYTICS_SERVER_URL`). When configuring via environment variables, `AMPLITUDE_SERVER_ZONE=EU` sets all four defaults.

### Remote evaluation mode flags

The proxy performs local evaluation only. Flags configured for **remote** evaluation mode are returned by Amplitude without targeting rules, so the proxy cannot evaluate them. As with local evaluation in the SDKs, the proxy returns their default variant, but it does not track assignment or exposure events for them. A warning is logged once per flag and the `amplitude_proxy_remote_evaluation_flag_skipped_total` metric is incremented. Evaluate such flags against Amplitude's remote evaluation endpoints directly.

### Tracking headers

* `X-Amp-Exp-Track: no-track` disables assignment event tracking for the evaluation (default: tracked).
* `X-Amp-Exp-Exposure-Track: track` enables exposure event tracking for the evaluation (default: not tracked).

### Security notes

* The container runs as a non-root user and does not enable remote JMX. JMX can be enabled explicitly via `JAVA_OPTS`; if you do, require authentication and restrict network access to the JMX port.
* There is no HTTP shutdown endpoint. The service shuts down gracefully on `SIGTERM`.
* Credentials (deployment keys, API keys, secret keys, management keys and Redis passwords) are redacted from logs.

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
