# %%
# Imports #

import datetime
import io
import json
import os
import sys

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# append grandparent
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import data_dir, grandparent_dir
from utils.display_tools import print_logger

# %%
# Load Env #

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


BRANCH_TYPE = os.getenv("BRANCH_TYPE")
print(f"BRANCH_TYPE: {BRANCH_TYPE}")


# if temp_creds have been created by aws cli
# Path to the JSON file
json_file = os.path.join(os.path.expanduser("~"), "aws_temporary_credentials.json")

if os.path.exists(json_file):
    print("Found temporary credentials file, exporting to env")

    # Read JSON file
    with open(json_file, "r") as f:
        # read raw text
        raw_text = f.read()
        # remove BOM characters
        fixed_text = raw_text.replace("ï»¿", "")
        data = json.loads(fixed_text)

    # Extract values
    aws_access_key_id = data["roleCredentials"]["accessKeyId"]
    aws_secret_access_key = data["roleCredentials"]["secretAccessKey"]
    aws_session_token = data["roleCredentials"]["sessionToken"]

    # Set environment variables
    os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
    os.environ["AWS_SESSION_TOKEN"] = aws_session_token


# %%
# Buckets #


# [branch_type][is_sensitive][bucket_owner]
dict_buckets = {
    "staging": {
        "sensitive": {
            "limesync": "hf-group-limesync-csv-staging-sensitive",
            "finance": "hf-us-finance-staging-sensitive",
            "finance-wd": "hf-us-finance-wd-staging-sensitive",
        },
        "nonsensitive": {
            "limesync": "hf-group-limesync-csv-staging-nonsensitive",
            "finance": "hf-na-finance-nonsensitive-staging",
        },
    },
    "live": {
        "sensitive": {
            "limesync": "hf-group-limesync-csv-live-sensitive",
            "finance": "hf-us-finance-live-sensitive",
            "finance-wd": "hf-us-finance-wd-live-sensitive",
        },
        "nonsensitive": {
            "limesync": "hf-group-limesync-csv-live-nonsensitive",
            "finance": "hf-na-finance-nonsensitive-live",
        },
    },
}


def get_s3_bucket(bucket_owner, is_sensitive):
    sensitivity = "sensitive" if is_sensitive else "nonsensitive"
    return dict_buckets[BRANCH_TYPE][sensitivity][bucket_owner]


dict_account_types = {
    "s3_bi_developer": "S3_CREDENTIALS_BI_DEVELOPER",
    "s3_uploader": "S3_CREDENTIALS_UPLOADER",
    "s3_uploader_2": "S3_CREDENTIALS_UPLOADER_2",
    "limesync": "S3_CREDENTIALS_LIMESYNC",
    "limesync_staging": "S3_CREDENTIALS_LIMESYNC_STAGING",
    "limesync_sensitive": "S3_CREDENTIALS_LIMESYNC_SENSITIVE",
    "limesync_staging_sensitive": "S3_CREDENTIALS_LIMESYNC_STAGING_SENSITIVE",
}


# %%
# Functions #


def get_s3_credentials(account_type):
    cred_env_key = dict_account_types[account_type]

    creds = json.loads(os.getenv(cred_env_key), strict=False)

    return creds


def get_s3_object(account_type):
    if account_type == "airflow_default":
        return boto3.client("s3")

    creds = get_s3_credentials(account_type)

    if account_type == "s3_bi_developer":
        s3 = boto3.client(
            "s3",
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
            aws_session_token=creds["aws_session_token"],
        )

    elif (
        account_type == "s3_uploader"
        or account_type == "s3_uploader_2"
        or account_type == "limesync"
        or account_type == "limesync_staging"
        or account_type == "limesync_sensitive"
    ):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
        )

    return s3


# %%
# Functions #


def list_buckets(account_type="s3_bi_developer"):
    s3 = get_s3_object(account_type)

    response = s3.list_buckets()
    buckets = [bucket["Name"] for bucket in response["Buckets"]]
    return buckets


def upload_file_to_s3(
    file_path,
    bucket_name,
    ls_save_path,
    account_type,
):
    start_time = datetime.datetime.now()

    save_path = "/".join(ls_save_path)
    write_path = f"s3a://{bucket_name}/{save_path}"

    print_logger(
        f"Uploading {file_path} to S3 bucket path {write_path} as {account_type}"
    )

    if account_type == "airflow_default":
        with open(file_path, "rb") as f:
            s3 = get_s3_object(account_type)
            s3.upload_fileobj(f, bucket_name, save_path)

    else:
        creds = get_s3_credentials(account_type)
        try:
            with open(file_path, "rb") as f:
                s3 = get_s3_object(account_type)
                s3.upload_fileobj(
                    f,
                    bucket_name,
                    save_path,
                    ExtraArgs={
                        "StorageClass": "STANDARD",
                        "ServerSideEncryption": "AES256",
                        "SSEKMSKeyId": creds["aws_kms_key_id"],
                    },
                )
        except Exception as e:
            print_logger(f"Error: {e}")
            with open(file_path, "rb") as f:
                s3 = get_s3_object(account_type)
                s3.upload_fileobj(
                    f,
                    bucket_name,
                    save_path,
                    ExtraArgs={
                        "StorageClass": "STANDARD",
                        "ServerSideEncryption": "AES256",
                    },
                )

    print_logger(
        f"Uploaded to {save_path}, after {datetime.datetime.now() - start_time}"
    )


def upload_dataframe_to_s3_as_parquet(
    df,
    bucket_name,
    ls_save_path,
    account_type,
):
    start_time = datetime.datetime.now()
    save_path = "/".join(ls_save_path)
    s3_key = f"{save_path}"

    print_logger(
        f"Uploading DataFrame to S3 bucket path {bucket_name}/{s3_key} as {account_type}"
    )

    # Convert the pandas DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(df)

    # Create a buffer
    buffer = io.BytesIO()

    # Write the table to the buffer as Parquet
    pq.write_table(table, buffer, compression="SNAPPY")

    # Move to the beginning of the buffer
    buffer.seek(0)

    # Initialize a boto3 client
    s3 = boto3.client("s3")

    # Upload the buffer content to S3
    s3.upload_fileobj(
        buffer,
        bucket_name,
        s3_key,
        ExtraArgs={"ContentType": "application/octet-stream"},
    )

    # Log the upload
    print_logger(
        f"Uploaded DataFrame as Parquet with Snappy to S3 bucket path {bucket_name}/{s3_key} as {account_type} "
        f"after {datetime.datetime.now() - start_time}"
    )


def upload_df_to_bucket(
    df,
    file_name,
    save_path,
    bucket_name,
    account_type,
):
    start_time = datetime.datetime.now()
    write_path = f"s3a://{bucket_name}/{save_path}"

    print(
        f"Uploading {file_name} to S3 bucket path {write_path} as {account_type},"
        f" with size {df.shape}"
    )

    if account_type == "airflow_default":
        df.to_csv(
            write_path,
            index=False,
        )

    else:
        creds = get_s3_credentials(account_type)
        df.to_csv(
            write_path,
            index=False,
            storage_options={
                "key": creds["aws_access_key_id"],
                "secret": creds["aws_secret_access_key"],
            },
        )

    print_logger(
        f"Uploaded to {save_path} with size {df.shape},"
        f" after {datetime.datetime.now() - start_time}"
    )


def get_file_from_bucket(
    bucket_name,
    ls_file_path,
    account_type,
):
    start_time = datetime.datetime.now()

    read_file_path = f"s3a://{bucket_name}/{'/'.join(ls_file_path)}"
    read_rel_path = "/".join(ls_file_path)
    save_path = os.path.join(data_dir, "s3_download_cache", *ls_file_path)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    print_logger(
        "Downloading "
        + read_file_path
        + " from S3 bucket "
        + bucket_name
        + " using account: "
        + account_type
    )

    s3 = get_s3_object(account_type)
    s3.download_file(bucket_name, read_rel_path, save_path)

    print_logger(
        "Downloaded to "
        + save_path
        + ", after "
        + str(datetime.datetime.now() - start_time)
    )

    return save_path


def list_s3_files_at_path(bucket_name, ls_file_path, account_type):
    s3 = get_s3_object(account_type)

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="/".join(ls_file_path))
    print(response)
    # List to store dictionaries for each file
    files_list = []

    for file in response.get("Contents", []):
        file_dict = {
            "file_name": file["Key"].split("/")[-1],
            "file_path": file["Key"],
            "updated_time": file["LastModified"],
        }
        files_list.append(file_dict)

    return files_list


def check_file_exists(bucket_name, ls_file_path, account_type):
    s3 = get_s3_object(account_type)

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="/".join(ls_file_path))

    return len(response.get("Contents", [])) > 0


def get_file_updated_time(bucket_name, ls_file_path, account_type):
    s3 = get_s3_object(account_type)

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="/".join(ls_file_path))

    return response.get("Contents", [])[0]["LastModified"]


# %%
# Test #

if __name__ == "__main__":
    print_logger("Testing s3_bi_developer")
    print(get_s3_object(account_type="s3_bi_developer"))

    print_logger("Testing s3_uploader")
    print(get_s3_object(account_type="s3_uploader"))

    print_logger("Testing s3_uploader_2")
    print(get_s3_object(account_type="s3_uploader_2"))

    print_logger("Testing limesync")
    print(list_buckets("limesync"))


# %%
