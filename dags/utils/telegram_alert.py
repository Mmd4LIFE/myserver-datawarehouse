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
    """Send a telegram message with robust error handling"""
    token, chat_id = _get_token_and_chat_id()
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set (env or Airflow Variables); skipping Telegram notification.")
        return

    try:
        # Create bot
        bot = telegram.Bot(token=token)
        
        # Send message with simple retry logic
        max_retries = 2
        for attempt in range(max_retries):
            try:
                bot.send_message(chat_id=chat_id, text=message)
                print(f"Telegram notification sent successfully: {message}")
                return
            except telegram.error.TimedOut:
                if attempt < max_retries - 1:
                    print(f"Telegram timeout on attempt {attempt + 1}, retrying...")
                    import time
                    time.sleep(2)  # Wait 2 seconds before retry
                    continue
                else:
                    print(f"Telegram notification failed after {max_retries} attempts due to timeout")
                    return
            except Exception as e:
                print(f"Telegram notification failed: {str(e)}")
                return
    except Exception as e:
        print(f"Failed to initialize Telegram bot: {str(e)}")
        return


def send_telegram_photo(photo_path: str, caption: str = "") -> None:
    """Send a photo to telegram with robust error handling"""
    token, chat_id = _get_token_and_chat_id()
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set (env or Airflow Variables); skipping Telegram photo.")
        return

    try:
        # Check if file exists
        if not os.path.exists(photo_path):
            print(f"Photo file not found: {photo_path}")
            return
            
        # Create bot
        bot = telegram.Bot(token=token)
        
        # Send photo with simple retry logic
        max_retries = 2
        for attempt in range(max_retries):
            try:
                with open(photo_path, 'rb') as photo_file:
                    bot.send_photo(
                        chat_id=chat_id, 
                        photo=photo_file, 
                        caption=caption[:1024] if caption else ""  # Telegram caption limit
                    )
                print(f"Telegram photo sent successfully: {photo_path}")
                return
            except telegram.error.TimedOut:
                if attempt < max_retries - 1:
                    print(f"Telegram photo timeout on attempt {attempt + 1}, retrying...")
                    import time
                    time.sleep(3)  # Wait 3 seconds before retry for photos
                    continue
                else:
                    print(f"Telegram photo failed after {max_retries} attempts due to timeout")
                    return
            except Exception as e:
                print(f"Telegram photo failed: {str(e)}")
                return
    except Exception as e:
        print(f"Failed to send photo to Telegram: {str(e)}")
        return


def send_telegram_photos(photo_paths: list, caption: str = "") -> None:
    """Send multiple photos to telegram as a media group"""
    token, chat_id = _get_token_and_chat_id()
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set (env or Airflow Variables); skipping Telegram photos.")
        return

    # Filter existing files
    existing_files = [path for path in photo_paths if os.path.exists(path)]
    if not existing_files:
        print("No photo files found to send")
        return
        
    try:
        # Create bot
        bot = telegram.Bot(token=token)
        
        # If only one photo, send as single photo
        if len(existing_files) == 1:
            send_telegram_photo(existing_files[0], caption)
            return
            
        # Send multiple photos with retry logic
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Prepare media group (max 10 photos per group in Telegram)
                media_group = []
                for i, photo_path in enumerate(existing_files[:10]):  # Limit to 10 photos
                    with open(photo_path, 'rb') as photo_file:
                        photo_caption = caption if i == 0 else ""  # Only first photo gets caption
                        media_group.append(
                            telegram.InputMediaPhoto(
                                media=photo_file.read(),
                                caption=photo_caption[:1024] if photo_caption else ""
                            )
                        )
                
                bot.send_media_group(chat_id=chat_id, media=media_group)
                print(f"Telegram media group sent successfully: {len(existing_files)} photos")
                return
            except telegram.error.TimedOut:
                if attempt < max_retries - 1:
                    print(f"Telegram media group timeout on attempt {attempt + 1}, retrying...")
                    import time
                    time.sleep(5)  # Wait 5 seconds before retry for media groups
                    continue
                else:
                    print(f"Telegram media group failed after {max_retries} attempts due to timeout")
                    # Fallback: send individual photos
                    print("Falling back to sending individual photos...")
                    for photo_path in existing_files:
                        send_telegram_photo(photo_path, f"{caption} - {os.path.basename(photo_path)}")
                    return
            except Exception as e:
                print(f"Telegram media group failed: {str(e)}")
                # Fallback: send individual photos
                print("Falling back to sending individual photos...")
                for photo_path in existing_files:
                    send_telegram_photo(photo_path, f"{caption} - {os.path.basename(photo_path)}")
                return
    except Exception as e:
        print(f"Failed to send photos to Telegram: {str(e)}")
        return


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
        return f"✅ {base} - Completed successfully"
    else:
        failed_tasks_str = f" (Failed tasks: {', '.join(failed_task_ids)})" if failed_task_ids else ""
        return f"❌ {base} - Failed{failed_tasks_str}"


def task_notify_success(context: Dict[str, Any]) -> None:
    """Telegram success notification with proper context handling"""
    try:
        message = _format_dag_status_message("success", context)
        send_telegram_message(message)
    except Exception as e:
        print(f"Error in task_notify_success: {str(e)}")


def task_notify_failure(context: Dict[str, Any]) -> None:
    """Telegram failure notification with proper context handling"""
    try:
        message = _format_dag_status_message("failure", context)
        send_telegram_message(message)
    except Exception as e:
        print(f"Error in task_notify_failure: {str(e)}")


# Legacy functions for backward compatibility
def task_notify_success_legacy(**context: Any) -> None:
    """Legacy success notification - deprecated, use task_notify_success instead"""
    task_notify_success(context)


def task_notify_failure_legacy(**context: Any) -> None:
    """Legacy failure notification - deprecated, use task_notify_failure instead"""
    task_notify_failure(context)