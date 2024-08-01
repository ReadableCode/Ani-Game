# %%
# Imports #

import json
import os
import sys

import pandas as pd
import requests
from dotenv import load_dotenv

# append grandparent
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import data_dir, grandparent_dir
from utils.display_tools import pprint_df  # noqa: F401

# %%
# Environment #

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

user_env_path = os.path.join(grandparent_dir, "user.env")
if os.path.exists(user_env_path):
    load_dotenv(user_env_path)


# %%
# Variables #

SLACK_USER_AUTH_TOKEN = os.getenv("SLACK_USER_AUTH_TOKEN")
SLACK_BOT_CONFIGURATIONS = os.getenv("SLACK_BOT_CONFIGURATIONS")

# %%
# Functions #


def get_slack_configs(slackbot_name):
    """
    Get the Slack configs from the JSON file.

    Returns:
    dict: The Slack configs.
    """

    return json.loads(SLACK_BOT_CONFIGURATIONS)[slackbot_name]


# %%
# Send Messages #


def send_slack_message(bot_name, text):
    """
    Send a message to a Slack channel using a webhook.

    Args:
    webhook_url (str): The Slack webhook URL.
    text (str): The message text to send.
    """

    configs = get_slack_configs(bot_name)
    webhook_url = configs["webhook_url"]

    payload = {
        "text": text,
    }

    headers = {"Content-Type": "application/json"}

    response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)

    if response.status_code != 200:
        raise ValueError(
            "Request to Slack returned an error "
            + f"{response.status_code}, the response is:\n{response.text}"
        )


def send_slack_message_with_bot_token(bot_name, text, channel, file_path=None):
    """
    Send a message to a Slack channel using Slack API for both text and file uploads.

    Args:
    bot_token (str): The Slack bot token.
    text (str): The message text to send.
    channel (str): The Slack channel ID to send the message to.
    file_path (str, optional): A file path to upload as an attachment. Defaults to None.
    """

    configs = get_slack_configs(bot_name)
    bot_token = configs["bot_token"]

    # Send the text message using the chat.postMessage API
    payload = {
        "token": bot_token,
        "channel": channel,
        "text": text,
    }

    response = requests.post(
        "https://slack.com/api/chat.postMessage",
        data=payload,
    )

    if not response.json()["ok"]:
        error_message = (
            f"Request to Slack returned an error {response.status_code}, "
            f"the response is:\n{response.text}"
        )
        raise ValueError(error_message)

    # Upload the file using the Slack API if a file_path is provided
    if file_path:
        # Upload the file to Slack
        files = {"file": open(file_path, "rb")}
        data = {"channels": channel}
        response = requests.post(
            "https://slack.com/api/files.upload",
            headers={"Authorization": f"Bearer {bot_token}"},
            data=data,
            files=files,
        )

        if not response.json()["ok"]:
            raise ValueError(
                "Request to Slack returned an error "
                + f"{response.status_code}, the response is:\n{response.text}"
            )


# %%
# Slack Thread Extraction #


# Download message thread between you and another person
def download_message_thread(token, conversation_id):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    params = {"channel": conversation_id}
    all_messages = []

    while True:
        response = requests.get(
            "https://slack.com/api/conversations.history",
            headers=headers,
            params=params,
        )

        if response.status_code != 200:
            raise ValueError(
                "Request to Slack returned an error "
                + f"{response.status_code}, the response is:\n{response.text}"
            )

        data = response.json()
        messages = data.get("messages", [])
        all_messages.extend(messages)

        # Check if there's a next cursor and update the params for the next request
        next_cursor = data.get("response_metadata", {}).get("next_cursor")
        if not next_cursor:
            break  # Exit loop if there's no next cursor

        params["cursor"] = next_cursor

    df_messages = pd.DataFrame(all_messages)
    return df_messages


# %%
# Send Messages With Bot Token #

if __name__ == "__main__":
    # Example text message
    text = (
        "Hello, this is a message from my Python script! here is a file "
        "https://docs.google.com/spreadsheets/d/1zbyEfvcKvOOA7nr1jyqlrF8mIa-4l25c/"
        "edit?usp=sharing&ouid=116937827583938060798&rtpof=true&sd=true"
    )

    send_slack_message(bot_name="finance_slack_bot", text=text)


# %%
# Send Messages With Webhook #

if __name__ == "__main__":
    # Example text message
    text = "Hello, this is a message from my Python script! <!here>"

    # Example Slack channel ID
    channel = "C051K79JX0T"

    # Example file path for attachment (optional)
    file_path = os.path.join(
        data_dir,
        "slack_attachments",
        "Supplier Index USDI - Detailed.xlsx 2023-04-03.xlsx",
    )

    send_slack_message_with_bot_token(
        bot_name="finance_slack_bot",
        text=text,
        channel=channel,
        file_path=file_path,
    )


# %%
