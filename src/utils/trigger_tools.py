# %%
# Imports #

import glob
import os
import sys

# append grandparent
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import trigger_dir
from utils.display_tools import print_logger

# %%
# Trigger #


def check_for_allowed_to_run(file_name_pattern):
    print_logger("Checking for lock file")
    if os.path.isfile(os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt")):
        print_logger("Lock file found")
        print_logger("Done")
        return False

    else:
        print_logger("Lock file not found")

        return True


def remove_lock_file(file_name_pattern):
    lock_file_path = os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt")
    if os.path.isfile(lock_file_path):
        print_logger("Removing lock file")
        os.remove(os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt"))


def create_lock_file(file_name_pattern):
    print_logger("Creating lock file")
    with open(os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt"), "w") as f:
        f.write("lock")


def check_for_trigger(file_name_pattern):
    print_logger("Checking for lock file")
    if os.path.isfile(os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt")):
        print_logger("Lock file found")
        print_logger("Done")
        return False

    else:
        print_logger("Lock file not found")

        ls_files = glob.glob(os.path.join(trigger_dir, f"{file_name_pattern}*.txt"))

        if ls_files:
            print_logger("Files found")

            print_logger("Creating lock file")
            with open(
                os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt"), "w"
            ) as f:
                f.write("lock")

            return True
        else:
            print_logger("No files found")
            print_logger("Done")
            return False


def get_ls_trigger_files(file_name_pattern):
    print_logger("Getting list of files triggering this script")
    ls_files = glob.glob(os.path.join(trigger_dir, f"{file_name_pattern}*.txt"))
    print_logger(f"Found {len(ls_files)} files")
    ls_files_without_lock_file = [
        file_item
        for file_item in ls_files
        if file_item != os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt")
    ]

    return ls_files_without_lock_file


def completed_trigger_run(file_name_pattern):
    print_logger("Script Complete")

    print_logger(
        "Moving all files triggering this script to done folder, except lock file"
    )
    ls_files = glob.glob(os.path.join(trigger_dir, f"{file_name_pattern}*.txt"))

    for file_item in ls_files:
        print_logger(f"Checking file: {file_item}")

        if file_item != os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt"):
            print_logger("This is not the lock file")
            print_logger("Moving file to done folder")
            orig_path_with_name = os.path.join(trigger_dir, os.path.basename(file_item))
            new_path_with_name = os.path.join(
                trigger_dir, "done", os.path.basename(file_item)
            )
            os.rename(
                orig_path_with_name,
                new_path_with_name,
            )

    print_logger("Removing lock file")
    os.remove(os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt"))
    print_logger("Done")


# %%
