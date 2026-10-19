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

### Redis cleanup for large cohorts

Configure Redis to free expired values in the background:

```conf
lazyfree-lazy-expire yes
```

This is a **Redis server setting**, not an evaluation proxy YAML option. For managed Redis,
set it in the provider's persistent configuration or parameter group. Apply and verify it on
every primary and replica that can serve proxy traffic, including nodes promoted after failover.
For self-managed Redis, keep it in `redis.conf`; the Docker Compose example passes it at startup.
Where permitted, `CONFIG GET lazyfree-lazy-expire` verifies the effective value on each node.
The proxy does not issue `CONFIG SET` or require permission to change server configuration.

After a successful cohort refresh, the proxy retains the old full member set and compressed
blob for `cohortSyncIntervalMillis` before they expire. This grace period protects reads that
started on the previous version. Pending member sets and legacy diff temporary sets also use
expiry to recover storage after interrupted refreshes. Without asynchronous expiry, freeing a
multi-million-member set can block unrelated Redis requests even with streamed diff enabled.

Explicit cohort deletion and legacy diff temporary-key cleanup use
[`UNLINK`](https://redis.io/docs/latest/commands/unlink/), which requires Redis 4.0+ and permission
to run that command. `UNLINK` preserves immediate key removal while freeing large values in the
background. It does **not** change TTL expiry behavior: `lazyfree-lazy-expire=yes` is still
required. The proxy keeps the existing retention and pending-version TTLs.

Allow memory headroom while background freeing completes. Monitor `lazyfree_pending_objects`,
Redis memory, and request latency during cohort updates. See the Redis
[lazy-free configuration](https://github.com/redis/redis/blob/7.0/redis.conf#L1124-L1173).

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
