# Contract: CLI Interface

**Command**: `hros-event-worker`

---

## 1. Syntax

```bash
hros-event-worker [command] [flags]
```

### Commands
- `run`: Start the outbox event worker for a specified service domain.
- `version`: Print version and build information.
- `help`: Help about any command.

---

## 2. Flags for `run` Command

| Flag | Shorthand | Type | Required | Default | Description |
|------|-----------|------|----------|---------|-------------|
| `--type` | `-t` | string | **Yes** | `""` | Service domain type (`setting`, `access`, `directory`) |
| `--config` | `-c` | string | No | `./config.yaml` | Path to YAML configuration file |

---

## 3. Exit Codes & Error Behaviors

| Exit Code | Reason | Example Stderr Output |
|-----------|--------|----------------------|
| `0` | Clean graceful shutdown on signal | `{"level":"info","message":"worker shutdown completed"}` |
| `1` | Configuration or validation error | `invalid worker type: foo. supported types: setting, access, directory` |
| `1` | Database/Kafka initialization failure | `failed to connect to database: dial tcp 127.0.0.1:5432: connect: connection refused` |

---

## 4. Environment Variables Mapping

| Environment Variable | Overrides Config Property |
|----------------------|---------------------------|
| `HROS_<TYPE>_DATABASE_HOST` | `<type>.database.host` |
| `HROS_<TYPE>_DATABASE_PORT` | `<type>.database.port` |
| `HROS_<TYPE>_DATABASE_NAME` | `<type>.database.name` |
| `HROS_<TYPE>_DATABASE_USER` | `<type>.database.user` |
| `HROS_<TYPE>_DATABASE_PASSWORD` | `<type>.database.password` |
| `HROS_<TYPE>_DATABASE_SSL_MODE` | `<type>.database.ssl_mode` |
| `HROS_<TYPE>_KAFKA_BROKERS` | `<type>.kafka.brokers` (comma-separated or list) |
| `HROS_<TYPE>_KAFKA_CLIENT_ID` | `<type>.kafka.client_id` |
| `HROS_<TYPE>_KAFKA_TOPIC_PREFIX` | `<type>.kafka.topic_prefix` |
| `HROS_<TYPE>_OUTBOX_BATCH_SIZE` | `<type>.outbox.batch_size` |
| `HROS_<TYPE>_OUTBOX_POLL_INTERVAL` | `<type>.outbox.poll_interval` |
| `HROS_<TYPE>_HTTP_PORT` | `<type>.http.port` |

*(where `<TYPE>` is `SETTING`, `ACCESS`, or `DIRECTORY`)*
