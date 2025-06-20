#!/usr/bin/python3
"""
description: contains basic custom functions

usage:
    from helpers.utils import get_logger
"""
import logging
from random import randint
from typing import List, Union
from math import log10
from pathlib import Path

from helpers.logger import get_stream_handler


def get_logger(name: str) -> logging.Logger:
    """
    Initializes a logging object to handle any print messages
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    # Uncomment to enable writing errors to a second file.
    # error_dir = Path(f"errors/tmp_{timestamp()}/")
    # error_log = f"{name}.err"
    # if not error_dir.is_dir():
    #     logger.error("Creating a new directory to store error logs...")
    #     error_dir.mkdir(parents=True)
    # logging_file = f"{str(error_dir)}/{error_log}"
    # logger.addHandler(get_file_handler(logging_file))
    logger.addHandler(get_stream_handler())
    return logger

def random_with_N_digits(n: int) -> int:
    """
    Create a number of an arbitrary length (n)
    """
    range_start = 10 ** (n - 1)
    range_end = (10**n) - 1
    return randint(range_start, range_end)


def generate_job_id() -> str:
    """
    Create a dummy slurm job id
    """
    return f"{random_with_N_digits(8)}"


def check_if_all_same(
    list_of_elem: List[Union[str, int]], item: Union[str, int]
) -> bool:
    """
    Using List comprehension, check if all elements in list are same and matches the given item.
    """
    return all([elem == item for elem in list_of_elem])


def find_NaN(list_of_elem: List[Union[str, int, None]]) -> List[int]:
    """
    Returns a list of indexs within a list which are 'None'
    """
    list = [i for i, v in enumerate(list_of_elem) if v == None]
    return list


def find_not_NaN(list_of_elem: List[Union[str, int, None]]) -> List[int]:
    """
    Returns a list of indexs within a list which are not 'None'
    """
    list = [i for i, v in enumerate(list_of_elem) if v != None]
    return list


def create_deps(num: int = 4) -> List[None]:
    """
    Create a list of None of a certain length.
    """
    return [None] * num

def collect_job_nums(
    dependency_list: List[Union[str, None]], allow_dep_failure: bool = False
) -> List[str]:
    """Format a list of Slurm job numbers into a SLURM dependency string, and build command flags for SBATCH.

    Parameters
    ----------
    dependency_list : List[str]
        contains 8-digit SLURM job numbers, uses 'None' as a placeholder; downstream jobs will run when these jobs finish
    allow_dep_failure : bool, optional
        if True, allow downstream jobs to start even if dependency returns a non-zero exit code; by default False

    Returns
    -------
    dependency_cmd: List[str]
        contains the SBATCH flags for job dependency
    """
    not_none_values = filter(None, dependency_list)
    complete_list = list(not_none_values)
    prep_jobs = ":".join(complete_list)
    if allow_dep_failure:
        dependency_cmd = [
            f"--dependency=afterany:{prep_jobs}",
            "--kill-on-invalid-dep=yes",
        ]
    else:
        dependency_cmd = [
            f"--dependency=afterok:{prep_jobs}",
            "--kill-on-invalid-dep=yes",
        ]
    return dependency_cmd


def phredGQ_to_Eprob(gq_value: int) -> float:
    """
    Convert reported GQ values back to error probabilities.

    Parameters
    ----------
    gq_value : int
        phred-scaled GQ value
    """
    error_prob = 10 ** (gq_value / -10)
    prop_error_prob = error_prob * 100
    print(f"ERROR PROB @ GQ={gq_value}:\t{prop_error_prob:.09f}%")

    return error_prob

# def clean_str(txt: str) -> str:
#     """
#     Handle any special characters and remove any separators.

#     Input: 'A,Quick brown-fox jumped-over-the   lazy-dog'
#     Output: 'AQuickbrownfoxjumpedoverthelazydog'
#     """
#     specialChars = "!#$%^&*()"
#     for specialChar in specialChars:
#         txt = txt.replace(specialChar, "")
#     standardizeSeps = " -,_"
#     for sep in standardizeSeps:
#         txt = txt.replace(sep, "")
#     return txt

def count_digits(number: int) -> int:
    """
    Count how many digits are in a numerical value.

    Args:
        number (int): any numerical value

    Returns:
        int: how many digits were found
    """
    return int(log10(abs(number))) + 1 if number else 1

def partial_match_case_insensitive(
    search_string:str = "app",
    list_of_strings: List[str] = ["Apple", "banana", "Cherry", "apricot", "grapefruit"]) -> Union[List[str], None]:
    """
    Finds partial string matches in a list, ignoring case.

    Args:
        search_string: The string to search for.
        list_of_strings: The list of strings to search within.

    Returns:
      A list of strings from the input list that contain the search string, ignoring case.
    """
    search_string = search_string.casefold()
    matches_found = [s for s in list_of_strings if search_string in s.casefold()]
    if matches_found:
        return matches_found
    else:
        return
    
# def check_file_case_insensitive(filename: Union[str, Path], expected_filename: str) -> Union[Path, None]:
#     """
#     Checks if a file exists, case-insensitively.

#     Args:
#         filename: The path to the existing file (string or Path object)
#         expected_filename: The name of the file to check (a lowercase string).

#     Returns:
#         True if the file exists (case-insensitive), False otherwise.
#     """
#     filepath = Path(filename).resolve()
#     target_lower = str(expected_filename).lower()
#     if filepath.is_file() and str(filepath).lower() == target_lower:
#         return filepath

def iterdir_with_prefix(absolute_path: Union[str,Path], prefix: str, valid_suffixes: List[str]):
    """
    Iterates over the files and directories in a given path that start with the specified prefix.

    Args:
        path: The path to iterate over.
        prefix: The prefix to filter by.

    Yields:
        Path: Path objects that match the prefix.
    """
    directory = Path(absolute_path)
    return [directory / item for item in directory.iterdir() if (item.name.startswith(prefix) and item.is_file() and item.suffix.lower() in valid_suffixes)]


