import os
import re
from collections import defaultdict
from urllib.parse import quote_plus

import glob2

import aisdb

psql_conn_string = f"postgresql://<USER>:{quote_plus('<PASSWORD>')}@<HOST>:<PORT>/<DATABASE>"


def process_csv(filepaths, source):
    """
    Process CSV files and store the data in a PostgreSQL database.

    Args:
        filepaths (list): List of filepaths of the CSV files to be processed.
        source (str): Source of the data to be stored in the database.

    Returns:
        None

    Raises:
        None

    Examples:
        filepaths = ['/path/to/file1.csv', '/path/to/file2.csv']
        source = 'example_source'
        process_csv(filepaths, source)
    """
    with aisdb.PostgresDBConn(libpq_connstring=psql_conn_string) as dbconn:
        aisdb.decode_msgs(filepaths, dbconn=dbconn, source=source, verbose=True, skip_checksum=True)


def group_files_by_month(zip_files, file_regex=r'(\d{2})(\d{4})'):
    """
    Group files by month.

    Parameters:
    - zip_files (list): A list of zip files.

    Optional parameters:
    - file_regex (str): A regular expression pattern used to extract the
                        year and month from the filename. The default pattern is r'(\d{2})(\d{4})'.

    Returns:
    - defaultdict: A defaultdict with keys representing year and month,
                   and values representing a list of files that belong to that month.

    Example:
    >>> zip_files = ['file_01012021.csv', 'file_02012021.csv', 'file_03012021.csv']
    >>> grouped_files = group_files_by_month(zip_files)
    >>> print(grouped_files)
    defaultdict(<class 'list'>, {<re.Match object; span=(5, 11), match='010120'>: ['file_01012021.csv'],
                <re.Match object; span=(5, 11), match='020120'>: ['file_02012021.csv'], <re.Match object;
                span=(5, 11), match='030120'>: ['file_03012021.csv']})
    """
    grouped_files = defaultdict(list)
    for file in zip_files:
        if "CSV" in file:
            year_month = re.search(file_regex, os.path.basename(file))
            grouped_files[year_month].append(file)
    return grouped_files


def find_and_process_zip_files_1(root_dir):
    """
    Find and process ZIP files containing CSV files within a given root directory.

    Args:
        root_dir (str): The root directory to search for ZIP files.

    Returns:
        None

    """
    zip_files = glob2.glob(os.path.join(root_dir, '**/*CSV*.zip'))
    grouped_files = group_files_by_month(zip_files)

    for k, v in grouped_files.items():
        if len(v) < 28 or len(v) > 31:
            print(f"Processing {k}: {len(v)}")

    for key in sorted(grouped_files.keys()):
        process_csv(grouped_files[key])


def find_and_process_zip_files_2(root_dir):
    """
    Find and process zip files and CSV files in the given directory.

    This method searches for all ZIP and CSV files in the specified root directory and its subdirectories.
    It then combines the list of found files and sorts them in ascending order.
    The  files are processed in batches of 10.

    Parameters:
        root_dir (str): The root directory to search for ZIP and CSV files.

    Returns:
        None

    Example usage:
        >>> find_and_process_zip_files_2('/path/to/directory')
    """
    zip_files = glob2.glob(os.path.join(root_dir, '**/*.zip'))
    csv_files = glob2.glob(os.path.join(root_dir, '**/*.csv'))
    all_files = sorted(set(list(zip_files) + list(csv_files)))
    for i in range(0, len(all_files), 10):
        batch_files = all_files[i:i + 10]
        process_csv(batch_files)


if __name__ == "__main__":
    root_dir = f"/PATH-TO-YOUR-RAW-DATA/"
    # find_and_process_zip_files_1(root_dir)
    # find_and_process_zip_files_2(root_dir)
