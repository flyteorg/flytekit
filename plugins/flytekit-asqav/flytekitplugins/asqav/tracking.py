import datetime
import os
from typing import Any, Callable, Dict, Optional, Union

import asqav
from flytekit import Deck, Secret
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.utils import ClassDecorator
from flytekit.loggers import logger

ASQAV_LINK_TYPE_KEY = "link_type"
ASQAV_DEFAULT_RECEIPT_TYPE = "protectmcp:lifecycle"
ASQAV_ENV_API_KEY = "ASQAV_API_KEY"

ASQAV_SCOPE_KEY = "scope"
ASQAV_AGENT_NAME_KEY = "agent_name"
ASQAV_RECEIPT_TYPE_KEY = "receipt_type"
ASQAV_RISK_CLASS_KEY = "risk_class"
ASQAV_COMPLIANCE_MODE_KEY = "compliance_mode"


def _receipt_html(receipt_type: str, sig_id: str, verification_url: str, metadata: Dict[str, Any]) -> str:
    """Render a single audit receipt as an HTML row."""
    status_icon = "✅" if receipt_type == "finished" else "❌" if receipt_type == "failed" else "⏳"
    rows = "".join(
        f"<tr><td style='padding: 4px 8px; font-weight: 600;'>{k}</td>"
        f"<td style='padding: 4px 8px;'>{v}</td></tr>"
        for k, v in metadata.items()
    )
    return f"""
    <div style="border: 1px solid #e0e0e0; border-radius: 8px; margin-bottom: 16px; overflow: hidden;">
        <div style="background: #f8f9fa; padding: 12px 16px; border-bottom: 1px solid #e0e0e0;
                    font-size: 16px; font-weight: 600;">
            {status_icon} Receipt: {receipt_type}
            <span style="float: right; font-size: 12px; color: #666;">ID: {sig_id}</span>
        </div>
        <div style="padding: 12px 16px;">
            <table style="width: 100%; border-collapse: collapse;">
                {rows}
            </table>
            <p style="margin-top: 12px;">
                <a href="{verification_url}" target="_blank"
                   style="background: #4f46e5; color: white; padding: 8px 16px; border-radius: 4px;
                          text-decoration: none; font-size: 13px;">
                    🔗 Verify on Asqav
                </a>
            </p>
        </div>
    </div>"""


class asqav_audit(ClassDecorator):
    """Cryptographic audit decorator for Flyte tasks using Asqav.

    Wraps a ``@task``-decorated function to sign cryptographically verifiable
    receipts at each lifecycle point (started, finished, failed). Receipts are
    rendered as a Flyte Deck card with verification links.

    Usage::

        from flytekit import task, Secret, workflow
        from flytekitplugins.asqav import asqav_audit

        asqav_secret = Secret(key="asqav-api-key", group="asqav")

        @task(secret_requests=[asqav_secret])
        @asqav_audit(agent_name="model-trainer", secret=asqav_secret)
        def train_model() -> float:
            ...
    """

    def __init__(
        self,
        task_function=None,
        *,
        agent_name: str,
        secret: Optional[Union[Secret, Callable]] = None,
        receipt_type: str = ASQAV_DEFAULT_RECEIPT_TYPE,
        risk_class: Optional[str] = None,
        compliance_mode: bool = True,
        **init_kwargs,
    ):
        if not agent_name:
            raise ValueError("agent_name must be set")
        super().__init__(
            task_function,
            agent_name=agent_name,
            secret=secret,
            receipt_type=receipt_type,
            risk_class=risk_class,
            compliance_mode=compliance_mode,
            **init_kwargs,
        )
        self.agent_name = agent_name
        self.secret = secret
        self.receipt_type = receipt_type
        self.risk_class = risk_class
        self.compliance_mode = compliance_mode

    def _resolve_api_key(self) -> str:
        """Resolve the Asqav API key from the most specific source available."""
        # 1. Callable — highest priority for programmatic injection
        if callable(self.secret):
            return self.secret()
        # 2. Environment variable — convenient for local development
        api_key = os.environ.get(ASQAV_ENV_API_KEY)
        if api_key:
            return api_key
        # 3. Flyte Secret — for remote execution with secret_requests
        if isinstance(self.secret, Secret):
            ctx = FlyteContextManager.current_context()
            return ctx.user_space_params.secrets.get(key=self.secret.key, group=self.secret.group)
        raise ValueError(
            f"No Asqav API key found. Provide a `Secret` or callable to "
            f"`asqav_audit(secret=...)`, or set the {ASQAV_ENV_API_KEY} env var."
        )

    def get_extra_config(self):
        extra_config = {
            ASQAV_LINK_TYPE_KEY: "asqav-audit",
            ASQAV_AGENT_NAME_KEY: self.agent_name,
            ASQAV_SCOPE_KEY: "flyte-task-lifecycle",
            ASQAV_RECEIPT_TYPE_KEY: self.receipt_type,
            ASQAV_COMPLIANCE_MODE_KEY: str(self.compliance_mode),
        }
        if self.risk_class:
            extra_config[ASQAV_RISK_CLASS_KEY] = self.risk_class
        return extra_config

    def execute(self, *args, **kwargs):
        ctx = FlyteContextManager.current_context()
        is_local = ctx.execution_state.is_local_execution()
        execution_id = str(ctx.user_space_params.execution_id) if not is_local else "local"

        api_key = self._resolve_api_key()

        # Initialise the Asqav SDK and create an agent
        asqav.init(api_key=api_key)
        agent = asqav.Agent.create(name=self.agent_name)

        receipts = []
        start_time = datetime.datetime.utcnow()

        # --- started receipt ---
        started_context = {
            "event_type": "flyte.task.started",
            "execution_id": execution_id,
            "agent_name": self.agent_name,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        started_resp = agent.sign(
            action_type="flyte.task.started",
            context=started_context,
            compliance_mode=self.compliance_mode,
            receipt_type=self.receipt_type,
            risk_class=self.risk_class,
        )
        receipts.append(("started", started_resp))
        logger.info(f"Signed asqav started receipt: {started_resp.signature_id}")

        # --- task execution ---
        output = None
        exception: Optional[Exception] = None
        try:
            output = self.task_function(*args, **kwargs)
        except Exception as e:
            exception = e
            raise
        finally:
            elapsed = (datetime.datetime.utcnow() - start_time).total_seconds()

            if exception is not None:
                error_info = f"{type(exception).__name__}: {exception}"
                failed_context = {
                    "event_type": "flyte.task.failed",
                    "execution_id": execution_id,
                    "agent_name": self.agent_name,
                    "duration_seconds": round(elapsed, 3),
                    "error": error_info,
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                }
                try:
                    failed_resp = agent.sign(
                        action_type="flyte.task.failed",
                        context=failed_context,
                        compliance_mode=self.compliance_mode,
                        receipt_type=self.receipt_type,
                        risk_class=self.risk_class,
                    )
                    receipts.append(("failed", failed_resp))
                    logger.info(f"Signed asqav failed receipt: {failed_resp.signature_id}")
                except Exception as sign_err:
                    logger.error(f"Failed to sign asqav receipt for error: {sign_err}")
            else:
                finished_context = {
                    "event_type": "flyte.task.finished",
                    "execution_id": execution_id,
                    "agent_name": self.agent_name,
                    "duration_seconds": round(elapsed, 3),
                    "output_type": type(output).__name__ if output is not None else "None",
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                }
                try:
                    finished_resp = agent.sign(
                        action_type="flyte.task.finished",
                        context=finished_context,
                        compliance_mode=self.compliance_mode,
                        receipt_type=self.receipt_type,
                        risk_class=self.risk_class,
                    )
                    receipts.append(("finished", finished_resp))
                    logger.info(f"Signed asqav finished receipt: {finished_resp.signature_id}")
                except Exception as sign_err:
                    logger.error(f"Failed to sign asqav receipt for success: {sign_err}")

            # Render Flyte Deck with all receipts
            if receipts:
                deck_html_parts = [
                    "<h3>Asqav Cryptographic Audit Trail</h3>",
                    f"<p><strong>Agent:</strong> {self.agent_name} | "
                    f"<strong>Execution ID:</strong> {execution_id}</p>",
                ]
                for rtype, resp in receipts:
                    metadata = {
                        "Action Type": resp.signature_id,
                        "Algorithm": resp.algorithm or "unknown",
                        "Compliance Mode": str(resp.compliance_mode),
                    }
                    if resp.receipt_type:
                        metadata["Receipt Type"] = resp.receipt_type
                    if resp.risk_class:
                        metadata["Risk Class"] = resp.risk_class
                    deck_html_parts.append(
                        _receipt_html(rtype, resp.signature_id, resp.verification_url, metadata)
                    )
                deck = Deck("Asqav Audit Trail", "\n".join(deck_html_parts))

        return output
