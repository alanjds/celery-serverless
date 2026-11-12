

# celery-serverless

**Deploy Celery workers as Serverless (AWS Lambda) functions.**

`celery-serverless` enables running Celery workers in a serverless architecture, primarily targeting AWS Lambda via the Serverless Framework. It implements a lightweight "Watchdog" pattern to monitor message queues, handle distributed locking, and dynamically invoke worker functions. This approach optimizes cold starts, reduces idle costs, and scales processing on-demand.

## Installation

```bash
pip install celery-serverless
```

## Quick Start

1. **Initialize** your project configuration:
   ```bash
   celery serverless init
   ```
   This generates a `serverless.yml` file and installs the required `serverless-python-requirements` plugin.

2. **Configure** your AWS credentials and broker settings in `serverless.yml`.

3. **Deploy** the worker and watchdog functions to AWS:
   ```bash
   celery serverless deploy
   ```

4. **Invoke** functions locally or in your deployed environment:
   ```bash
   celery serverless invoke
   ```

## CLI Commands

The package registers as a native Celery subcommand:

| Command | Description |
|---------|-------------|
| `celery serverless init` | Scaffolds `serverless.yml` and installs necessary Serverless plugins. |
| `celery serverless deploy` | Packages and deploys the Lambda functions to the configured cloud provider. |
| `celery serverless invoke` | Invokes the worker or watchdog. Automatically selects `serverless` CLI or `boto3` strategy. |

## Configuration

The library relies on a `serverless.yml` file and runtime environment variables to manage queue monitoring, locking, and inter-process communication.

| Environment Variable | Description |
|----------------------|-------------|
| `CELERY_SERVERLESS_QUEUE_URL` | URL of the message broker/queue to monitor (e.g., `redis://`). Set to `disabled` to skip queue monitoring. |
| `CELERY_SERVERLESS_INTERCOM_URL` | URL used for internal worker communication and state tracking. |
| `CELERY_SERVERLESS_LOCK_URL` | URL for distributed locking to prevent watchdog collisions across concurrent invocations. |
| `CELERY_SERVERLESS_QUEUES` | Comma-separated list of Celery queues to listen to (default: `celery`). |
| `CELERY_SERVERLESS_LOGLEVEL` | Controls the logging verbosity for Lambda handlers. |
| `CELERY_SERVERLESS_SHUTDOWN_KEY` | Redis key pattern used to trigger a graceful watchdog shutdown. |

## Extras & Integrations

Optional dependencies can be installed via pip extras to extend functionality:

```bash
pip install celery-serverless[sentry,wdb,logdrain,s3conf,boto3]
```

| Extra | Description | Required Env Var |
|-------|-------------|------------------|
| `sentry` | Error tracking & exception capture via Sentry | `SENTRY_DSN` |
| `wdb` | Remote web debugging support | `WDB_SOCKET_URL` |
| `logdrain` | Syslog log forwarding (UDP/TCP) | `LOGDRAIN_URL` |
| `s3conf` | Environment configuration management via AWS S3 | `S3CONF` |
| `boto3` | Direct AWS Lambda invocation via `boto3`/`aioboto3` | AWS Credentials configured |

Disable auto-detection for any extra by setting `CELERY_SERVERLESS_NO_<EXTRA>=1`.

## Development & Testing

The project supports Python 3.5 and 3.6. Tests require a local Redis instance.

```bash
# Install in editable mode with test dependencies
pip install -e ".[test]"

# Run the test suite (requires Redis running on default port)
redis-server &
tox
```

## License

Apache Software License 2.0

## Authors

Alan Justino & Samuel Barbosa Neto  
📧 alan.justino@yahoo.com.br  
🌐 https://github.com/alanjds/celery-serverless
