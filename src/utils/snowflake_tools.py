# %%
# Imports #

import json
import os
import sys
import time

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

# append grandparent
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import grandparent_dir, query_dir
from utils.display_tools import print_logger

# %%
# Creds #

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

user_dotenv_path = os.path.join(grandparent_dir, "user.env")
if os.path.exists(user_dotenv_path):
    load_dotenv(user_dotenv_path)


# %%
# Roles #

dict_roles = {
    "people_insights_user": "US_PEOPLE_INSIGHTS_USER",
    "people_insights_python_role": "SRV_US_PEOPLE_INSIGHTS_PYTHON_ROLE",
    "prodtech": "US_PRODTECH_USER",
    "scm_user": "SCM_DATA_USER",
    "ops_analytics": "US_OPS_ANALYTICS_USER",
    "fin_ops": "SRV_FINANCE_FIN_OPS_COST_REPORTING_SA_ROLE",
}


def get_role(role_type_or_role_name):
    if role_type_or_role_name in dict_roles.keys():
        return dict_roles[role_type_or_role_name]
    else:
        return role_type_or_role_name


# %%
# User Password Auth #

dict_account_types = {
    "personal": "SNOWFLAKE_CREDS_USER",
    "people_insights_service_account": (
        "SNOWFLAKE_CREDS_PEOPLE_INSIGHTS_SERVICE_ACCOUNT"
    ),
    "prod_tech_service_account": ("SNOWFLAKE_CREDS_PROD_TECH_SERVICE_ACCOUNT"),
    "fin_ops_service_account": (
        "SNOWFLAKE_CREDS_FIN_OPS_COST_REPORTING_SERVICE_ACCOUNT"
    ),
}


def get_snowflake_credentials(account_type, role_type, warehouse=""):
    role_to_use = get_role(role_type)

    cred_env_key = dict_account_types[account_type]

    creds = json.loads(os.getenv(cred_env_key), strict=False)

    if warehouse == "":
        warehouse = creds["warehouse"]

    print_logger(
        "Getting Credentials with:\n"
        f"User: {creds['user']}\n"
        f"Account: {creds['account']}\n"
        f"Warehouse: {creds['warehouse']}\n"
        f"Role: {role_to_use}",
        level="info",
    )

    if account_type == "personal":
        ctx = snowflake.connector.connect(
            user=creds["user"],
            account=creds["account"],
            warehouse=warehouse,
            role=role_to_use,
            authenticator="externalbrowser",
        )
    elif "private_key" in creds.keys():
        print("Using private key")
        private_key_data = creds["private_key"].replace("\\n", "\n").strip()

        # Load the PEM key
        private_key_pem = serialization.load_pem_private_key(
            private_key_data.encode(),
            password=None,
            backend=default_backend(),
        )

        # Convert the key to DER format
        der_private_key = private_key_pem.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        ctx = snowflake.connector.connect(
            user=creds["user"],
            account=creds["account"],
            warehouse=warehouse,
            role=role_to_use,
            private_key=der_private_key,
        )
    else:
        ctx = snowflake.connector.connect(
            user=creds["user"],
            password=creds["password"],
            account=creds["account"],
            warehouse=warehouse,
            role=role_to_use,
        )

    print_logger(
        f"Got Credentials with:\n"
        f"User: {creds['user']}\n"
        f"Account: {creds['account']}\n"
        f"Warehouse:{warehouse}\n"
        f"Role: {role_to_use}",
        level="info",
    )

    return ctx


# %%
# Get Raw Data from Snowflake #


def list_tables(account_type, role_type):
    role_to_use = get_role(role_type)

    ctx = get_snowflake_credentials(account_type, role_to_use)

    cs = ctx.cursor()

    try:
        cs.execute("SHOW TABLES")
        tables = cs.fetchall()
    finally:
        cs.close()
    ctx.close()

    return tables


def query_snowflake(account_type, role_type, query_to_run, warehouse="", max_retries=5):
    print_logger(f"Running query: {query_to_run}", level="info")

    attempt = 0
    delay = 60  # Start with a 1 minute delay
    while attempt <= max_retries:
        ctx = get_snowflake_credentials(account_type, role_type, warehouse=warehouse)
        cs = ctx.cursor()
        try:
            cs.execute(query_to_run)
            query_output = ctx.cursor().execute(query_to_run).fetch_pandas_all()
            return query_output
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise Exception(f"All {max_retries} retries failed") from e
            print_logger(
                f"Snowflake query Attempt {attempt} failed: {e}. Retrying in {delay / 60:.2f} minutes...",
                level="warning",
            )
            time.sleep(delay)
            delay = min(600, delay * 2)
        finally:
            cs.close()
            ctx.close()


def executeScriptsFromFile(
    account_type,
    role_type,
    filename,
    multi_part=False,
):
    # Open and read the file as a single buffer
    fd = open(os.path.join(query_dir, filename), "r")
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    if multi_part:
        sqlCommands = sqlFile.split(";")
    else:
        sqlCommands = [sqlFile]

    # Execute every command from the input file
    for command in sqlCommands:
        result = query_snowflake(account_type, role_type, command)

    return result


# %%
