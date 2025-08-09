import os
import telegram
from typing import Any, Dict, List

from dotenv import load_dotenv

# Airflow imports are optional at import-time to keep this module usable in non-Airflow contexts
try:
    from airflow.utils.state import State
except Exception:  # pragma: no cover - Airflow may not be available in some contexts
    State = None  # type: ignore

load_dotenv()


def _get_token_and_chat_id() -> (str, str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if token and chat_id:
        return token, chat_id

    # Fallback to Airflow Variables if available
    try:
        from airflow.models import Variable  # type: ignore

        token = token or Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
        chat_id = chat_id or Variable.get("TELEGRAM_CHAT_ID", default_var=None)
    except Exception:
        pass

    return token, chat_id


def send_telegram_message(message: str) -> None:
    token, chat_id = _get_token_and_chat_id()
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set (env or Airflow Variables); skipping Telegram notification.")
        return

    bot = telegram.Bot(token=token)
    bot.send_message(chat_id=chat_id, text=message)


def _format_dag_status_message(status: str, context: Dict[str, Any]) -> str:
    dag_id = context.get("dag").dag_id if context.get("dag") else "<unknown_dag>"
    run_id = context.get("run_id", "<unknown_run>")
    ts = context.get("ts") or "<unknown_time>"

    # Collect failed tasks if Airflow context is available
    failed_task_ids: List[str] = []
    try:
        dag_run = context.get("dag_run")
        if dag_run and State is not None:
            failed_task_ids = [
                ti.task_id for ti in dag_run.get_task_instances() if getattr(ti, "state", None) == State.FAILED
            ]
    except Exception:
        # Best-effort; ignore formatting issues
        failed_task_ids = []

    base = f"{dag_id}"

    if status.lower() == "success":
        return f"✅ {base}"
    else:
        return f"❌ {base}"


def task_notify_success(**context: Any) -> None:
    message = _format_dag_status_message("success", context)
    send_telegram_message(message)


def task_notify_failure(**context: Any) -> None:
    message = _format_dag_status_message("failure", context)
    send_telegram_message(message)