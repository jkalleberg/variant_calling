#!/usr/bin/python3
"""
description: custom function for removing multiple file suffixes.

usage:
    from helpers.suffix import remove_suffixes
"""
from pathlib import Path
from typing import List,Union

def remove_suffixes(
    filename: Path,
    remove_all: bool = True,
    suffixes: Union[None, List[str]] = None) -> Path:
    """
    Removing multiple file suffixes.
    """
    if suffixes is None:
        if not remove_all:
            _suffixes = {".gz"}
        else:
            _suffixes = {".bcf", ".vcf", ".gz"}
    else:
        _suffixes = set(suffixes)
        # print("SUFFIXES:", _suffixes)
    
    while filename.suffix in _suffixes:
        filename = filename.with_suffix("")

    return filename
