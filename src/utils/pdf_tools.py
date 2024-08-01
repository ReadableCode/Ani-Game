# %%
# Imports #

import os
import re
import sys

import fitz
import pandas as pd
import pytesseract
import tabula
from PyPDF2 import PdfReader

# append grandparent
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import data_dir
from utils.display_tools import pprint_ls, print_logger

# %%
# Functions #


def get_images_from_pages(pdf_file_path):
    ls_paths_images_from_pages = []

    root_filename = os.path.basename(pdf_file_path).replace(".pdf", "")

    # Create a PDF object
    pdf = fitz.open(pdf_file_path)

    # fo each page
    for page in pdf.pages():
        page_number = page.number

        pix = page.get_pixmap()
        output = os.path.join(data_dir, f"{root_filename}-{page_number}.png")
        pix.save(output)
        ls_paths_images_from_pages.append(output)

    print(f"Returning {len(ls_paths_images_from_pages)} images from {pdf_file_path}")
    return ls_paths_images_from_pages


def extract_text_from_image(image_file_path):
    text = pytesseract.image_to_string(image_file_path)
    return text


def find_dates_in_string(text):
    dates = re.findall(r"\d{1,2}\/\d{1,2}\/\d{4}", text)

    return dates


def find_dates_in_pdf_file(pdf_file_path):
    ls_paths_images_from_pages = get_images_from_pages(pdf_file_path)
    pprint_ls(ls_paths_images_from_pages)

    for image_file in ls_paths_images_from_pages:
        text = extract_text_from_image(image_file)

        ls_dates_from_file = find_dates_in_string(text)

        return ls_dates_from_file


def extract_table_from_pdf(pdf_path):
    number_pages = len(get_images_from_pages(pdf_path))
    file_name = os.path.basename(pdf_path)
    print_logger(
        f"Working on file: {file_name}, Number of pages in pdf: {number_pages}",
        as_break=True,
    )
    df_table = pd.DataFrame()
    for pdf_page_num in range(1, number_pages + 1):
        ls_dfs_from_page = tabula.read_pdf(pdf_path, pages=pdf_page_num)
        for df_from_page in ls_dfs_from_page:
            if not df_from_page.empty:
                # Create a new DataFrame with numbered columns
                new_columns = [
                    f"Column_{i + 1}" for i in range(len(df_from_page.columns))
                ]
                # Create a DataFrame containing the original column names as a data row
                header_row_data = pd.DataFrame(
                    [df_from_page.columns], columns=new_columns
                )

                # replace column names in df_from_page
                df_from_page.columns = new_columns

                # Concatenate the header row, new_df, and the original data row-wise
                df_from_page = pd.concat(
                    [header_row_data, df_from_page], axis=0, ignore_index=True
                )

                df_table = pd.concat([df_table, df_from_page], ignore_index=True)

    return df_table, number_pages


def find_contacts(pdf_path):
    """
    Extracts text from a PDF file, searches for unique phone numbers and email addresses,
    and returns a pandas DataFrame containing the information.
    """
    with open(pdf_path, "rb") as f:
        reader = PdfReader(f)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
    # regular expression for email addresses (handles leading numbers)
    email_pattern = (
        r"(?<![a-zA-Z0-9_.+-])([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)"
    )
    # regular expression for phone numbers (optional customization)
    phone_number_pattern = (
        r"(\d{3}[-\.\s]\d{3}[-\.\s]\d{4})"  # Adjust pattern as needed
    )
    # Find all potential phone numbers and email addresses using refined patterns
    phone_numbers = re.findall(phone_number_pattern, text)
    emails = re.findall(email_pattern, text)
    # Pair phone numbers with email addresses, if available
    contact_info = []
    for phone_number in phone_numbers:
        email = emails.pop(0) if emails else "Not Found"
        contact_info.append((phone_number, email))

    # Create DataFrame from the paired contact information
    df = pd.DataFrame(contact_info, columns=["Phone Number", "Email"])

    # Add the filename as a column
    df["Filename"] = pdf_path

    # Drop duplicates based on phone number
    df = df.drop_duplicates(subset="Phone Number", keep="first")

    return df


# %%
