#!/usr/bin/python3
"""
description: start here to begin cohort calling with a custom DeepVariant checkpoint.

usage: python3 run.py                                               \
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

from helpers.utils import iterdir_with_prefix
from helpers.module_builder import CustomModule
from helpers.inputs import InputManager
from pipeline.input import PipelineInputManager
from pipeline.pipeline import Pipeline
from pipeline.genome import Genome


def __init__() -> None:

    # Ensure that the --output-path flag is included
    run = CustomModule(output_required=True)
    run.start_module()

    run._parser.add_argument(
        "--keep-jobids",
        dest="benchmark",
        help="if True, save SLURM job numbers for calculating resource usage",
        action="store_true",
    )
    run._parser.add_argument(
        "--model-config",
        dest="model_config",
        help="[REQUIRED]\ninput file(s) (.json)\ndefines internal parameters for a specific variant caller\nto use multiple variant callers, provide a comma-separated list config files\n(default: %(default)s)",
        default="./tutorial/data/cattle/default_config.json",
        type=str,
        metavar="</path/file>",
    )
    run._parser.add_argument(
        "-m",
        "--modules",
        dest="modules",
        help="[REQUIRED]\ninput file (.sh)\nhelper script which loads the local software packages\n(default: %(default)s)",
        default="./scripts/setup/modules.sh",
        type=str,
        metavar="</path/file>",
    )
    run._parser.add_argument(
        "-r",
        "--resources",
        dest="resource_config",
        help="[REQUIRED]\ninput file (.json)\ndefines HPC cluster resources for SLURM",
        default="./tutorial/data/resources.json",
        type=str,
        metavar="</path/file>",
    )
    run._parser.add_argument(
        "-R",
        "--reference-prefix",
        dest="ref_file",
        help="[REQUIRED]\ninput file prefix;\ndefines naming convention for the reference genome to find similar file(s) located in the same directory;\nminimum file expectations:\n\t(.fasta + .fai index)\n\tnaming convention used to create a reference dictionary file with PICARD (.dict) and a default regions file (.bed), if these are missing.", 
        type=str,
        metavar="</path/file_prefix_only>", 
    )
    run._parser.add_argument(
        "--submit-size",
        dest="submit_size",
        help="controls the number of samples' submitted for variant calling;\n effectively rate-limits the amount of SLURM jobs submitted\n(default: %(default)s)",
        type=int,
        metavar="<int>",
        default=1,
    )
    run._parser.add_argument(
        "--submit-start",
        dest="submit_start",
        help="1-based index representing the first row of --input-path to include\n(default: %(default)s)",
        type=int,
        metavar="<int>",
        default=1,
    )
    run._parser.add_argument(
        "--submit-stop",
        dest="submit_stop",
        help="1-based index representing the final row of --input-path to include\n(default: %(default)s)",
        type=int,
        metavar="<int>",
        default=1,
    )
    run._parser.add_argument(
        "--unmapped-reads",
        dest="unmapped_reads",
        help="[REQUIRED]\nprefix for unmapped reads in reference genome; used to exclude from these during variant calling\ndefaults to @SQ tag from ARS-UCD1.2_Btau5.0.1Y\n(default: %(default)s)",
        type=str,
        metavar="<str>",
        default="NKLS",
    )

    # Uncomment to force arg entry at command line
    run.collect_args()

    # Edit for manually testing command line arguments
    # run.collect_args(
    #     [
    #         "-O",
    #         "../CATTLE_TEST/",
    #         # "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TEST/output.txt", # WILL BREAK
    #         "-I",
    #         # "/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/data/", # WILL BREAK
    #         # "/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/data/2.txt", # WILL BREAK
    #         # "./tutorial/data/240711_9913_1kbulls_ars1.2.samples.csv",
    #         "./tutorial/data/250627_Sutovsky_samples.csv",
    #         "--reference-prefix",
    #         # "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/REF_GENOME_COPY/ARS-UCD1.2.fai", # WILL BREAK!
    #         "../REF_GENOME_COPY/ARS-UCD1.2_Btau5.0.1Y",
    #         # "--dry-run",
    #         # "--debug",
    #         # "--overwrite",
    #         # UNCOMMENT / EDIT TO CONFIRM DIFFERENT FILE(S) or VALUES WORK
    #         # THESE DO NOT WORK YET!
    #         # "--submit-size",
    #         # # "2",
    #         # "10",
    #         # # "--submit-start",
    #         # # "2",
    #         # "--submit-stop",
    #         # # "2",
    #         # "10",
    #         #########################
    #         # "--resources",
    #         # "tutorial/data/resources.json",
    #         # "--modules",
    #         # "./scripts/setup/modules.sh",
    #         # "--unmapped-reads",
    #         # "NKLS",
    #     ]
    # )

    # Confirm that user-provided command line arguments are valid
    try:
        # Check generic command-line flags -------------------------
        run.check_args()
        
        # Save the generic command line arguments for convenience
        run.process_args()

        # INPUT PATH: Determine if a directory name was given as input, when it should be a file
        # Confirm that a file.ext format was entered        
        assert (
            run._args.in_path.stem != run._args.in_path.name and run._args.in_path.is_file()
        ), f"invalid --input-path; expected a file, did you enter a directory? | '{run._args.in_path}'"

        # OUTPUT PATH: Determine if a file name was given as output, when it should be a directory
        assert (
            run._args.out_path.stem == run._args.out_path.name and run._args.out_path.is_dir()
        ), f"invalid --output-path; expected a directory, did you enter a file? | '{run._args.out_path}'"

        ### Check custom command line flags ------------------------
        # FLEXIBLE START/STOP ARGS: terminate the pipelines if submit_stop value is less than submit_stop
        assert (
            run._args.submit_start <= run._args.submit_stop
        ), f"--submit-start={run._args.submit_start:,} must be less than or equal to --submit-stop={run._args.submit_stop:,}'"

        # COMPUTE ENVIRONMENT FLAGS: terminate the pipeline if it can run on the system
        # Make the --modules flag [REQUIRED]
        assert (
            run._args.modules
        ), f"missing [REQUIRED] flag: --modules; Please provide the path to an existing modules.sh file containing HPC-cluster-specific software dependencies"

        # Resolve any relative path entered for modules.sh
        _resolved_module_path = Path(run._args.modules).resolve()

        # Confirm path provide is valid
        assert (
            _resolved_module_path.is_file()
        ), f"unable to find the modules file | '{_resolved_module_path}'"
        run._args.modules = _resolved_module_path

        #  Make the --resources flag [REQUIRED]
        assert (
            run._args.resource_config
        ), "missing [REQUIRED] flag: --resources; Please provide the path to an existing JSON file containing SLURM SBATCH flags"

        # Resolve any relative path entered for --resources
        _resolved_resource_path = Path(run._args.resource_config).resolve()

        # Confirm path provide is valid
        assert _resolved_resource_path.is_file(), f"unable to find the resource config JSON file | '{_resolved_resource_path}'"
        run._args.resource_config = _resolved_resource_path

        # PIPELINE SPECIFIC INPUTS: terminate the pipeline if analysis inputs were entered incorrectly
        #########################
        #   Reference genome    #
        #########################
        # Make the flag --reference-prefix [REQUIRED]
        assert (
            run._args.ref_file
        ), "missing [REQUIRED] flag: --reference-prefix; Please provide <path/prefix_only> for a reference genome (.FASTA)"

        # Resolve any relative path entered for --reference-prefix
        _resolved_ref_path = Path(run._args.ref_file).resolve()

        # Confirm that the both a FASTA and INDEX file are available
        _reference_files = iterdir_with_prefix(
            absolute_path=_resolved_ref_path.parent,
            prefix=_resolved_ref_path.name,
            valid_suffixes=[
                ".fasta",
                ".fa",
                ".fai",
                ".dict",
                ".FASTA",
                ".FA",
                ".FAI",
                ".DICT",
            ],
        )

        assert (
            len(_reference_files) > 2
        ), f"unable to find at least two reference genome files (.FASTA + .FAI) | '{run._args.ref_file}'"

        # Confirm that a .fai index file exists prior to running PICARD CreateSequenceDict()
        for file in _reference_files:
            if "fai" in file.suffix.lower():
                # Update the command-line args to point to the FASTA file as a Path() object
                run._args.ref_file  = Path(file).parent / Path(file).stem

        # Confirm path provide is valid
        _files_found = ",".join([str(r) for r in _reference_files])
        assert (
            run._args.ref_file.is_file()
        ), f"missing a .fai index file in reference genome directory entered | '{run._args.ref_file.parent}'.\nFiles found: '{_files_found}'"

        #########################################
        #   Variant Calling Model Config(s)     #
        #########################################
        # Make the flag --model-config [REQUIRED]
        assert (
            run._args.model_config
        ), "missing [REQUIRED] flag: --model-config; Please provide:\n 1) the path to an model config (.JSON)\n 2) a comma-separated list of model config (.JSON) files"

        # Convert a potential comma-separated string of model config files into a iterable list
        if "," in run._args.model_config:
            _config_list = run._args.model_config.split(",")
        else:
            _config_list = [run._args.model_config]

        # Resolve potential relative paths for config file(s)
        _resolved_configs = [Path(c).resolve() for c in _config_list]

        # Confirm the user provided valid config file(s)
        for config in _resolved_configs:
            assert (
                config.is_file()
            ), f"missing a --model-config file | '{config}'.\nPlease provide the path to at least one existing model config (.JSON) file."

        # Update the command line args to be a list of Path() objects
        run._args.model_config = _resolved_configs

    except AssertionError as error:
        run._logger.error(f"{run._logger_msg}: {error}.\nExiting... ")
        exit(1)

    # Save the generic command line arguments for convenience
    # run.process_args()

    # Handle custom inputs needed for the generic variant calling pipeline
    _cl_inputs = InputManager(
        args=run._args,
        logger=run._logger,
        phase="setup",
    )
    _cl_inputs.update_mode()
    _cl_inputs.create_logging_msg()

    # Save valid command-line args for con
    _cl_inputs._output_path = run._output_path
    _cl_inputs._input_path = run._input_path

    # Load in SLURM resource config file
    _cl_inputs.load_slurm_resources()
    # TO DO: add a check_resources function to make sure format is correct?

    # Prepare pipeline-specific inputs
    _pipeline_inputs = PipelineInputManager(cl_inputs=_cl_inputs)

    try:
        # Load in the list of model config file(s)
        _pipeline_inputs.load_model_configs()

        # Confirm the model config file(s) match expectations
        _pipeline_inputs.check_model_configs()

        # produce the manual for DeepVariant
        if _pipeline_inputs._get_help is True:
            # Only need to init a Genome() but won't run anything
            g = tuple([0, [None, None]])
            _genome = Genome(
                sample=g,
                pipeline_inputs=_pipeline_inputs,
            )
            _genome.init_genome()
            _group_of_samples = Pipeline(
                pipeline_inputs=_pipeline_inputs,
                submit_size=_cl_inputs.args.submit_size,
            )
            _group_of_samples.process_genome(
                genome=_genome, get_help=_pipeline_inputs._get_help
            )

    except AssertionError as error:
        run._logger.error(f"{run._logger_msg}: {error}.\nExiting... ")
        exit(1)

    # Create a PICARD reference .dict file, if necessary
    _pipeline_inputs.find_ref_dict()
    _pipeline_inputs.transform_dictionary()

    # Create a 'default regions BED file' to exclude the unmapped contigs
    _pipeline_inputs.default_regions_BED()

    # Determine how many rows were provided as a single input
    _pipeline_inputs.count_inputs()  

    # Define a temp file to save samples to re-run
    # NOTE: this isn't currently used because there isn't a --check-samples flag (yet)
    # _pipeline_inputs.create_new_sample_file()

    # If --benchmark=True, define a temp file to store SLURM job ids for compute/wall time benchmarking
    _pipeline_inputs.create_benchmarking_file()

    # Load in the original samples file
    _pipeline_inputs.process_input_file()

    # Update phase
    _cl_inputs.phase = f"variant_calling"
    _cl_inputs.create_logging_msg()

    # Begin to iterate through all the samples    
    _group_of_samples = Pipeline(pipeline_inputs = _pipeline_inputs,
                                 submit_size=_cl_inputs.args.submit_size)
    _group_of_samples.process_cohort()

    # print("STOP!")
    # breakpoint()

    run.end_module()


# Execute functions created
if __name__ == "__main__":
    __init__()
