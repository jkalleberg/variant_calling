#!/usr/bin/python3
"""
description: template to use for creating custom Python executables.

example: python3 run.py -O /path/to/output/dir -I /path/to/input/file --dryrun --debug --overwrite

"""

from pathlib import Path
from sys import path

abs_path = Path(__file__).resolve()
module_path = str(abs_path.parent.parent)
path.append(module_path)

from helpers.module_builder import CustomModule

def __init__() -> None:

    run = CustomModule()
    run.start_module()

    # Edit for manually testing command line arguments
    run.collect_args(
        [
            "-O",
            "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TEST",
            "-I",
            "/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CUE_RESULTS/SAMPLES/240711_9913_1kbulls_ars1.2.samples.csv",
            "--dry-run",
            "--debug",
            "--overwrite",
        ]
    )
    
    run.collect_args()
    run.check_args()
    breakpoint()
    run.process_args()
    
    # ENTER CUSTOM SUB-MODULES HERE!
    
    run.end_module()


# Execute functions created
if __name__ == "__main__":
    __init__()
