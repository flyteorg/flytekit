# Flytekit Asqav Plugin

Cryptographic audit trails for Flyte tasks using [Asqav](https://asqav.com).

Each task execution is wrapped with cryptographically signed receipts at lifecycle
points (started, finished, failed), providing provable, tamper-proof records for
AI workflows in regulated environments (finance, healthcare, etc.).

## Installation

```bash
pip install flytekitplugins-asqav
```

Requires `flytekit>=1.12.0` and `asqav>=0.1.0`.

## Quick Start

```python
from flytekit import task, Secret, workflow
from flytekitplugins.asqav import asqav_audit

asqav_secret = Secret(key="asqav-api-key", group="asqav")

@task(secret_requests=[asqav_secret])
@asqav_audit(agent_name="model-trainer", secret=asqav_secret)
def train_model() -> float:
    # ... training code ...
    return 0.95

@workflow
def main() -> float:
    return train_model()
```

## Configuration

### `@asqav_audit(...)`

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `agent_name` | `str` | ✅ | — | Asqav agent name for this task |
| `secret` | `Secret` or `Callable` | ❌ | `ASQAV_API_KEY` env var | How to resolve the API key |
| `receipt_type` | `str` | ❌ | `protectmcp:lifecycle` | Asqav receipt type |
| `risk_class` | `str` | ❌ | `None` | Risk classification (`low`, `medium`, `high`, `unknown`) |
| `compliance_mode` | `bool` | ❌ | `True` | Generate IETF-compliant compliance receipts |
| `fail_closed` | `bool` | ❌ | `False` | If `True`, Asqav errors propagate (fail-closed); if `False`, errors are logged and the task continues (fail-open) |

### Secret Resolution (priority order)

1. **`Secret` object** — resolved via Flyte's `SecretsManager` (for remote deployment)
2. **Callable** — zero-argument function returning the API key string
3. **`ASQAV_API_KEY`** environment variable — fallback

## Audit Trail Output

Each receipt is logged and rendered as a Flyte Deck card with:
- Receipt type (started / finished / failed)
- Execution ID and timestamp
- Duration (for finished/failed receipts)
- Error info (for failed receipts)
- Clickable verification link to Asqav

## License

Apache 2.0
