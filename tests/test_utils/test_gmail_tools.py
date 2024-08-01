# %%
# Imports #

import os
from os.path import expanduser
import sys
import pytest
import pandas as pd
import numpy as np

import config_test_utils

from src.utils.gmail_tools import get_gmail_service

from src.utils.display_tools import pprint_ls, pprint_df, print_logger

# %%
# Tests #


def test_get_gmail_service():
    gmail_service = get_gmail_service(
        os.path.join(expanduser("~"), "credentials", "personal", "gmail_auth")
    )
    assert gmail_service is not None
    print_logger(f"gmail_service: {gmail_service}")


# %%
# Main #

if __name__ == "__main__":
    test_get_gmail_service()


# %%
