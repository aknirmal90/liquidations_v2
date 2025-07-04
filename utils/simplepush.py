import requests
from decouple import config


def send_simplepush_notification(
    title: str, message: str, event: str = "event"
) -> requests.Response:
    """
    Send a notification using SimplePush API.

    Args:
        title: Notification title
        message: Notification message
        event: Event type (default: "event")

    Returns:
        requests.Response: The response from the SimplePush API
    """
    url = "https://api.simplepush.io/send"
    data = {
        "key": config("SIMPLEPUSH_KEY"),
        "title": title,
        "msg": message,
        "event": event,
    }
    return requests.post(url, data=data)
