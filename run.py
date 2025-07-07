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
from os import getenv

abs_path = Path(__file__).resolve()
module_path = str(abs_path.parent.parent)
path.append(module_path)

from helpers.utils import partial_match_case_insensitive, check_if_all_same, iterdir_with_prefix
from helpers.module_builder import CustomModule
from helpers.inputs import InputManager
from pipeline.input import PipelineInputManager
from pipeline.pipeline import Pipeline
from pipeline.genome import Genome


def __init__() -> None:

    # Ensure that the --output-path flag is included
    run = CustomModule(output_required=True)
    run.start_module()

    # Add custom command line flags:
    run._parser.add_argument(
        "-A",
        "--allele-freq",
        dest="pop_file",
        help="input file (.vcf.gz);\n[REQUIRED] when using the custom, bovine-trained DeepVariant (model.ckpt-282383);\nprovides the population allele frequencies to encode as an additional channel during variant calling", 
        type=str,
        metavar="</path/file>", 
    )
    run._parser.add_argument(
        "--get-help",
        dest="get_help",
        help="if True, provide the internal flags for DeepVariant and exit\n(default: %(default)s)",
        default=False,
        action="store_true",
    )
    run._parser.add_argument(
        "--keep-jobids",
        dest="benchmark",
        help="if True, save SLURM job numbers for calculating resource usage",
        action="store_true",
    )
    run._parser.add_argument(
        "-M",
        "--model-prefix",
        dest="model_prefix",
        help="[REQUIRED]\ninput file prefix;\ndefines naming convention for variant calling checkpoint(s) to use for variant calling\nto use multiple variant callers, provide a comma-separated list of checkpoint prefixes\n(default: %(default)s)",
        default="./tutorial/existing_ckpts/DeepVariant/v1.4.0_withIS_withAF_bovid/model.ckpt-282383",
        type=str,
        metavar="</path/file_prefix_only> or </path/file_prefix_only1>,</path/file_prefix_only2>",
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
    # run.collect_args()

    # Edit for manually testing command line arguments
    run.collect_args(
        [
            "-O",
            "../CATTLE_TEST/",
            # "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TEST/output.txt", # WILL BREAK
            "-I",
            # "/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/data/", # WILL BREAK
            # "/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/data/2.txt", # WILL BREAK
            # "./tutorial/data/240711_9913_1kbulls_ars1.2.samples.csv",
            "./tutorial/data/250627_Sutovsky_samples.csv",
            "--reference",
            # "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/REF_GENOME_COPY/ARS-UCD1.2.fai", # WILL BREAK!
            "../REF_GENOME_COPY/ARS-UCD1.2_Btau5.0.1Y",
            ### NO --allele-freq with default ckpt will break!
            "--allele-freq",
            "../TRIOS_220704/POPVCF/UMAG1.POP.FREQ.vcf.gz",
            # "--dry-run",
            # "--debug",
            "--overwrite",
            # UNCOMMENT / EDIT TO CONFIRM DIFFERENT FILE(S) or VALUES WORK
            # "--get-help",
            
            # THESE DO NOT WORK YET!
            # "--submit-size",
            # # "2",
            # "10",
            # # "--submit-start",
            # # "2",
            # "--submit-stop",
            # # "2",
            # "10",
            #########################
            
            # "--resources",
            # "tutorial/data/resources.json",
            # "--modules",
            # "./scripts/setup/modules.sh",
            # "--unmapped-reads",
            # "NKLS",
            # "--model-prefix",
            # "./tutorial/existing_ckpts/DeepVariant/v1.4.0_withIS_default/model.ckpt",
        ]
    )

    # Confirm that user-provided command line arguments are valid
    try:
        # Check generic command-line flags -------------------------
        run.check_args()

        # INPUT PATH: Determine if a directory name was given as input, when it should be a file
        # Confirm that a file.ext format was entered
        assert (
            run._args.in_path.stem != run._args.in_path.name and run._args.in_path.is_file()
        ), f"invalid --input-path; expected a file, did you enter a directory? | '{run._args.in_path}'"

        # OUTPUT PATH: Determine if a file name was given as output, when it should be a directory
        assert (
            run._args.out_path.stem == run._args.out_path.name and run._args.out_path.is_dir()
        ), f"invalid --input-path; expected a file, did you enter a directory? | '{run._args.out_path}'"

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

        #############################################
        #   Variant Calling Model Checkpoint(s)     #
        #############################################
        # Convert a potential comma-separated string of checkpoint prefixes into a iterable list
        if "," in run._args.model_prefix:
            _ckpt_list = run._args.model_prefix.split(",")
        else:
            _ckpt_list = [run._args.model_prefix]

        # Check for supported variant callers
        _use_deepvariant = partial_match_case_insensitive("deepvariant", _ckpt_list)
        _use_cue = partial_match_case_insensitive("cue", _ckpt_list)

        # Confirm at least one supported variant caller was provided
        _no_valid_checkpoint = check_if_all_same([_use_deepvariant, _use_cue], None)
        assert (_no_valid_checkpoint is False), f"unable to find a supported checkpoint (e.g., DeepVariant or Cue) | '{run._args.model_prefix}'"

        # Get the expected default checkpoint path (custom bovid-trained WGS AF)
        _default_ckpt_prefix = Path(run.get_arg_default("model_prefix")).resolve()

        # Create an empty list to store valid checkpoint paths
        _list_of_ckpt_prefixes = list()

        if _use_deepvariant and len(_use_deepvariant) == 1:

            # Get the value of 'BIN_VERSION_DV', return None if not set
            _dv_version = getenv("BIN_VERSION_DV")

            # Confirm this environment variable exists
            assert (
                _dv_version is not None
            ), f"missing [REQUIRED] environment variable: ($BIN_VERSION_DV); Please double check that this variable is included in your modules.sh file"

            # Do not allow the user to deviate from v1.4.0
            assert (
                _dv_version == "1.4.0"
            ), f"invalid environment variable ($BIN_VERSION_DV); Please edit your modules.sh file to use the expected version of DeepVariant"
            # NOTE: In future, newer versions may become supported, but as they are untested, we do not encourage deviating from this expectation.

            # Identify the DeepVariant checkpoint prefix entered
            _user_ckpt_prefix = Path(_use_deepvariant[0]).resolve()

            # Determine if using the pipeline's default DeepVariant checkpoint (model.ckpt-282383),
            if _user_ckpt_prefix == _default_ckpt_prefix:

                # If so, make the flag --allele-freq [REQUIRED]
                assert (
                    run._args.pop_file
                ), "missing [REQUIRED] flag: --allele-freq; Please add a PopVCF to use the custom bovine-trained checkpoint (model.ckpt-282383)" 

                # Resolve any relative path entered for --allele-freq
                _resolved_pop_path = Path(run._args.pop_file).resolve()

                # Confirm the PopVCF file is available
                assert (_resolved_pop_path.is_file() is True), f"unable to find the PopVCF file | '{_resolved_pop_path}'"
                run._args.pop_file = _resolved_pop_path

                _list_of_ckpt_prefixes.append(_user_ckpt_prefix)

            else:
                print("ADD LOGIC FOR DIFFERENT DEEPVARIANT CHECKPOINTS")
                breakpoint()

            # Confirm that all the expected DeepVariant v1.4 checkpoint files are available
            _checkpoint_files = iterdir_with_prefix(
                absolute_path=_user_ckpt_prefix.parent,
                prefix=_user_ckpt_prefix.name,
                valid_suffixes=[".data-00000-of-00001", ".json", ".index", ".meta",],
                )

            assert (len(_checkpoint_files) == 4), f"unable to find all four DeepVariant checkpoint files | '{_user_ckpt_prefix}'"

        if _use_cue:
            print("ADD LOGIC CUE CHECKPOINT")
            breakpoint()

        # Confirm that a model checkpoint was entered
        assert (len(_list_of_ckpt_prefixes) >= 1), f"unable to find at least one valid checkpoint | '{_user_ckpt_prefix}'"

        # Save the list as a new command-line argument
        run._args.model_prefix = _list_of_ckpt_prefixes

    except AssertionError as error:
        run._logger.error(f"{run._logger_msg}: {error}.\nExiting... ")
        exit(1)

    # Save the generic command line arguments for convenience
    run.process_args()

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

    # Determine the variant caller(s) requested by the user
    # Currently supported valid options:
    #   DeepVariant v1.4.0
    # In future, we plan to support:
    #   DeepVariant v1.5.0+
    #   DeepTrio v1.5.0
    #   Cue v####
    # NOTE: this process expects the input checkpoint to be formatted as:
    #       ./tutorial/existing_ckpts/<MODEL_TYPE>/<MODEL_VERSION>/<CHECKPOINT_NAME>

    # Save info about the model(s) requested
    _variant_callers = dict()
    for ckpt in _ckpt_list:
        _checkpoint_path = Path(ckpt).resolve()
        _model_type = _checkpoint_path.parent.parent.name
        _model_version = _checkpoint_path.parent.name
        _checkpoint_name = _checkpoint_path.name
        _variant_callers[_model_type] = {"version": _model_version,
                                         "checkpoint_name": _checkpoint_name,
                                         "checkpoint_path": _checkpoint_path.parent}

    # Prepare pipeline-specific inputs
    _pipeline_inputs = PipelineInputManager(cl_inputs=_cl_inputs,
                                            variant_callers=_variant_callers)

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

    # produce the manual for DeepVariant
    if _cl_inputs.args.get_help:
        # Only need to init a Genome() but won't run anything
        g = tuple(list(_group_of_samples.pipeline_inputs._all_genomes.items())[0])
        _genome = Genome(
            sample=g, pipeline_inputs=_group_of_samples.pipeline_inputs,
            )
        _genome.init_genome()
        _group_of_samples.process_genome(genome=_genome, get_help=_cl_inputs.args.get_help)
    else:
        _group_of_samples.process_cohort()

    print("STOP!")
    breakpoint()

    run.end_module()


# Execute functions created
if __name__ == "__main__":
    __init__()
