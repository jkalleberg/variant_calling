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

from helpers.utils import partial_match_case_insensitive, check_if_all_same, iterdir_with_prefix
from helpers.module_builder import CustomModule
from helpers.inputs import InputManager
from pipeline.input import PipelineInputManager
from pipeline.pipeline import Pipeline


def __init__() -> None:

    run = CustomModule()
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
        "--group-start",
        dest="group_start",
        help="1-based index that controls which row to begin processing with --input\n(default: %(default)s)",
        type=int,
        metavar="<int>",
        default=1,
    )
    run._parser.add_argument(
        "--group-size",
        dest="group_size",
        help="controls the number of samples' submitted for variant calling;\n effectively rate-limits the amount of SLURM jobs submitted\n(default: %(default)s)",
        type=int,
        metavar="<int>",
        default=1,
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
            "./tutorial/data/240711_9913_1kbulls_ars1.2.samples.csv",
            
            "--reference",
            # "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/REF_GENOME_COPY/ARS-UCD1.2.fai", # WILL BREAK!
            "../REF_GENOME_COPY/ARS-UCD1.2_Btau5.0.1Y",
            ### NO --allele-freq with default ckpt will break!
            "--allele-freq", 
            "../TRIOS_220704/POPVCF/UMAG1.POP.FREQ.vcf.gz",
            
            "--dry-run",
            # "--debug",
            # "--overwrite",
            
            # UNCOMMENT / EDIT TO CONFIRM DIFFERENT FILE(S) or VALUES WORK
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
    
    try:
        # Check generic command-line flags
        run.check_args()
        
        # Check custom command line flags 
        # Make the --modules flag [REQUIRED]
        assert (
            run._args.modules
        ), f"missing [REQUIRED] flag: --modules; Please provide the path to an existing modules BASH file containing HPC-cluster-specific software dependencies"
        
        # Resolve any relative path entered for modules.sh
        _resolved_module_path = Path(run._args.modules).resolve()
        
        # Confirm path provide is valid
        assert _resolved_module_path.is_file(), f"unable to find the modules file | '{_resolved_module_path}'"
        run._args.modules = _resolved_module_path
        
        # Make the --resources flag [REQUIRED] 
        assert (
            run._args.resource_config
        ), "missing [REQUIRED] flag: --resources; Please provide the path to an existing JSON file containing SLURM SBATCH flags"

        # Resolve any relative path entered for --resources
        _resolved_resource_path = Path(run._args.resource_config).resolve()
        
        # Confirm path provide is valid 
        assert _resolved_resource_path.is_file(), f"unable to find the resource config JSON file | '{_resolved_resource_path}'"
        
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
            valid_suffixes=[".fasta", ".fa", ".fai", ".dict", ".FASTA", ".FA", ".FAI", ".DICT"],
            )
        
        assert (len(_reference_files) > 2), f"unable to find at least two reference genome files (.FASTA + .FAI) | '{run._args.ref_file}'"
        
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

        # print("MODEL PREFIX:", run._args.model_prefix)
        # print("DEEP VARIANT:", _use_deepvariant)
        # print(type(_use_deepvariant[0]))
        # # print("CUE:", _use_cue)
        # breakpoint()
        
        # Create an empty list to store valid checkpoint paths
        _list_of_ckpt_prefixes = list()

        if _use_deepvariant and len(_use_deepvariant) == 1:
            
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
            
            # Confirm that all the expected checkpoint files are available
            # print("ABSOLUTE PATH:", _user_ckpt_prefix.parent)
            # print("PREFIX:", _user_ckpt_prefix.name)
            # breakpoint()
            _checkpoint_files = iterdir_with_prefix(
                absolute_path=_user_ckpt_prefix.parent,
                prefix=_user_ckpt_prefix.name,
                valid_suffixes=[".data-00000-of-00001", ".json", ".index", ".meta",],
                )
            # print("CHECKPOINT FILES:", _checkpoint_files)
            # print("NUMBER OF CHECKPOINT FILES:", len(_checkpoint_files))
            # breakpoint()
            
            assert (len(_checkpoint_files) == 4), f"unable to find all four DeepVariant checkpoint files | '{_user_ckpt_prefix}'"
        
        if _use_cue:
            print("ADD LOGIC CUE CHECKPOINT")
            breakpoint()
        
        assert (len(_list_of_ckpt_prefixes) >= 1), f"unable to find at least one valid checkpoint | '{_user_ckpt_prefix}'"
            
        # Save the list as a new command-line argument
        # run._args.model_prefix = [Path(p).resolve() for p in _ckpt_list]
        run._args.model_prefix = _list_of_ckpt_prefixes
        
    except AssertionError as error:
        run._logger.error(f"{error}.\nExiting... ")
        exit(1)
    
    # Initialize all command line inputs
    run.process_args()
    
    # Handle custom inputs needed for the generic variant calling pipeline
    _cl_inputs = InputManager(
        args=run._args,
        logger=run._logger,
        phase="setup",
    )
    _cl_inputs.update_mode()
    _cl_inputs.create_logging_msg()
     
    # REFERENCE: Confirm a .fai index file exists prior to running PICARD CreateSequenceDict()
    for file in _reference_files:
        if "fai" in file.suffix.lower():
            # Update the command-line args to point to the FASTA file as a Path() object
            run._args.ref_file  = Path(file).parent / Path(file).stem
        
    if isinstance(run._args.ref_file, Path) and run._args.ref_file.is_file():
        if _cl_inputs.debug_mode:
            run._logger.debug(f"{_cl_inputs.logger_msg}: valid --reference FASTA file | '{run._args.ref_file}'")
    else:
        _files_found = ",".join(_reference_files)
        run._logger.error(f"{_cl_inputs.logger_msg}: missing a .fai index file in reference genome directory\nFiles found: {_files_found}")
    
    # INPUT PATH: Determine if a directory name was given as input, when it should be a file
    if run._input_path.stem != run._input_path.name:
        # Confirm input is an existing file
        if run._input_path.is_file():
            if _cl_inputs.debug_mode:
                run._logger.debug(f"{_cl_inputs.logger_msg}: valid --input; detected an existing file.")
            _cl_inputs._input_path = run._input_path
        else:
            run._logger.error(f"{_cl_inputs.logger_msg}: invalid --input; unable to find a sample CSV file | {run._input_path}\nExiting...")
            exit(1)
    else:
        # TO DO: enable providing an input directory (e.g., samples + metadata together)?
        run._logger.error(f"invalid --input; expected a file, did you enter a directory? | {run._input_path}\nExiting...")
        exit(1)

    # OUTPUT PATH: Determine if a file name was given as output, when it should be a directory
    if run._output_path.stem != run._output_path.name:
        run._logger.error(f"invalid --output-path; expected a directory, did you enter a file? | {run._output_path}\nExiting...")
        exit(1)
    else:
        # Create a new directory, if necessary 
        if not run._output_path.exists():
            _cl_inputs.create_a_dir(dir_name=run._output_path)
        else:
            if _cl_inputs.debug_mode:
                run._logger.debug(f"{_cl_inputs.logger_msg}: valid --output-path; detected an existing directory.")

        # Save valid command-line inputs
        _cl_inputs._output_path = run._output_path
                
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
    _pipeline_inputs.create_new_sample_file()
    
    # Define a temp file to store SLURM job ids for compute/wall time benchmarking
    _pipeline_inputs.create_benchmarking_file()
    
    # Load in the original samples file
    _pipeline_inputs.process_input_file()
    
    # Update phase
    _cl_inputs.phase = f"variant_calling"
    _cl_inputs.create_logging_msg()
    
    # Begin to iterate through all the samples
    _group_of_samples = Pipeline(pipeline_inputs = _pipeline_inputs)
    _group_of_samples.all_genomes()
    
    print("STOP!")
    breakpoint()
    
    run.end_module()


# Execute functions created
if __name__ == "__main__":
    __init__()
