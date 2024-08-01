# %%
# Imports #

import os
import sys

import hvac
import requests
from dotenv import load_dotenv

# append grandparent
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import grandparent_dir

# %%
# Variables #


dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

user_env_path = os.path.join(grandparent_dir, "user.env")
if os.path.exists(user_env_path):
    load_dotenv(user_env_path)

VAULT_URL = os.getenv("VAULT_URL")

dict_client_type_to_secrets_locs = {
    "na-finops": {
        "namespace_env_var": "VAULT_NAMESPACE",
        "token_env_var": "VAULT_TOKEN",
    },
    "airflow": {
        "namespace_env_var": "VAULT_NAMESPACE_AIRFLOW",
        "token_env_var": "VAULT_TOKEN_AIRFLOW",
    },
}


# %%
# Auth Functions #


def get_vault_namespace_and_token(client_type):
    vault_namespace = os.getenv(
        dict_client_type_to_secrets_locs[client_type]["namespace_env_var"]
    )
    vault_token = os.getenv(
        dict_client_type_to_secrets_locs[client_type]["token_env_var"]
    )
    return vault_namespace, vault_token


def get_vault_client(client_type):
    vault_namespace, vault_token = get_vault_namespace_and_token(client_type)

    # Initialize the Vault client
    client = hvac.Client(
        url=VAULT_URL,  # Replace with your Vault server URL
        namespace=vault_namespace,
        token=vault_token,  # Replace with your initial Vault token
    )

    return client


# %%
# Vault Functions #


def list_vault_mounts(client_type):
    api_endpoint = f"{VAULT_URL}/v1/sys/mounts"
    vault_namespace, vault_token = get_vault_namespace_and_token(client_type)

    headers = {
        "X-Vault-Token": vault_token,
        "X-Vault-Namespace": vault_namespace,
    }

    response = requests.get(api_endpoint, headers=headers)

    if response.status_code == 200:
        data = response.json()
        mounts = data["data"]

        print("Enabled Secret Engines and Mount Points:")
        for mount_point, mount_config in mounts.items():
            print(f"{mount_point}: {mount_config['type']}")

        return mounts

    else:
        print(f"Failed to list mounts. Status code: {response.status_code}")


def list_secrets(client_type, mount_point, secret_path):
    client = get_vault_client(client_type)
    try:
        response = client.secrets.kv.v2.read_secret_version(
            path=secret_path, mount_point=mount_point, raise_on_deleted_version=True
        )

        if "data" in response:
            data = response["data"]
            print("Successfully read secrets:")
            for key, value in data["data"].items():
                print(f"key: {key}")
            return data["data"]
        else:
            print(f"Failed to read secret. Status code: {response.status_code}")
    except Exception as e:
        print(f"Connection to Vault failed: {e}")


def get_vault_secret(client_type, mount_point, secret_path, secret_key):
    client = get_vault_client(client_type)
    try:
        response = client.secrets.kv.v2.read_secret_version(
            path=secret_path, mount_point=mount_point, raise_on_deleted_version=True
        )

        if "data" in response:
            data = response["data"]
            if secret_key in data["data"]:
                return data["data"][secret_key]
            else:
                print(f"Secret key {secret_key} not found.")
        else:
            print(f"Failed to read secret. Status code: {response.status_code}")
    except Exception as e:
        print(f"Connection to Vault failed: {e}")


# %%
# Function to add or update a secret in Vault #


def add_or_update_vault_secret(client_type, mount_point, secret_path, secret_data):
    client = get_vault_client(client_type)
    try:
        # Retrieve the existing secret if it exists
        try:
            response = client.secrets.kv.v2.read_secret_version(
                path=secret_path, mount_point=mount_point, raise_on_deleted_version=True
            )
            existing_data = response["data"]["data"]
        except hvac.exceptions.InvalidPath:
            # Secret does not exist yet, initialize with empty dict
            existing_data = {}

        # Update the existing data with new key-value pairs
        existing_data.update(secret_data)

        # Write the updated secret back to Vault
        response = client.secrets.kv.v2.create_or_update_secret(
            path=secret_path, mount_point=mount_point, secret=existing_data
        )

        print("Secret successfully added or updated.")
        return response
    except Exception as e:
        print(f"Failed to add or update secret: {e}")


# %%
