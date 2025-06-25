#!/usr/bin/python3
"""
description: after run.py, ensure large temp files are wiped after successful variant calling with the custom DeepVariant checkpoint.

usage: python3 archive                                              \
        -O ../CATTLE_TEST/                                          \
        -I ./tutorial/data/240711_9913_1kbulls_ars1.2.samples.csv   \
        --reference ../REF_GENOME_COPY/ARS-UCD1.2_Btau5.0.1Y        \
        --allele-freq ../TRIOS_220704/POPVCF/UMAG1.POP.FREQ.vcf.gz  \
        --dry-run                                                   \
                    
"""

from pathlib import Path
from sys import path, exit

abs_path = Path(__file__).resolve()
module_path = str(abs_path.parent.parent)
path.append(module_path)

from helpers.module_builder import CustomModule

# from os import path
# from pathlib import Path
# from sys import exit
# from typing import TYPE_CHECKING

# if TYPE_CHECKING:
#     from genome import Genome

# from _args import check_args, collect_args, get_args
# from clean_results import CleanUp
# from helpers.files import WriteFile
# from helpers.inputs import Inputs
# from helpers.utils import get_logger
# from helpers.wrapper import Wrapper, timestamp


def __init__() -> None:
    archive = CustomModule()
    archive.start_module()

    # input_file = Path(args.genome_pickle)

    # pickle_file = WriteFile(
    #     path_to_file=input_file.parent,
    #     file=input_file.name,
    #     inputs=inputs,
    # )
    # _genome = pickle_file.load_pickle()

    # try:
    #     assert _genome, "unable to re-open the pickled Genome()"
    #     assert isinstance(_genome, Genome), "unable to re-open the pickled Genome()"

    #     # update the 'mode' based on current script args
    #     _genome.iter.inputs = inputs

    #     clean_files = CleanUp(genome=_genome)
    #     clean_files.remove_lg_intermediates()

    # except AssertionError as e:
    #     logger.error(f"{inputs.logger_msg} {e}\nExiting...")

    archive.end_module()


# Execute functions created
if __name__ == "__main__":
    __init__()