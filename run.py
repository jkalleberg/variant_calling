#!/usr/bin/python3
"""
description: template to use for creating custom Python executables.

example: python3 run.py -O /path/to/output/dir -I /path/to/input/file --dryrun --debug --overwrite

"""

from pathlib import Path
from sys import path, exit

abs_path = Path(__file__).resolve()
module_path = str(abs_path.parent.parent)
path.append(module_path)

# from helpers.files import File
from helpers.utils import partial_match_case_insensitive, check_if_all_same, iterdir_with_prefix
from helpers.module_builder import CustomModule

def __init__() -> None:

    run = CustomModule()
    run.start_module()
    
    # Add custom command line flags:
    run._parser.add_argument(
        "-A",
        "--allele-freq",
        dest="pop_file",
        help="input file (.vcf.gz);\n[REQUIRED] when using the custom, bovine-trained version of DeepVariant v1.4.0;\nprovides the population allele frequencies to encode as an additional channel during variant calling", 
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
        help="[REQUIRED]\nabsolute path with checkpoint prefix only for variant calling model(s)\neither a single path, or provide a comma-separated list of paths for multiple variant callers\n(default: %(default)s)",
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
        "--reference",
        dest="ref_file",
        help="[REQUIRED]\ninput file (.fa/.fasta/.fasta.fai/.dict);\npoints to reference genome file(s) located in the same directory;\nminimum expectations:\n\t(.fa/.fasta with .fai index)\n\tused to create a PICARD reference dictionary file (.dict) and a default regions file (.bed) usings the reference prefix, unless these exist already.", 
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
            "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TEST/",
            # "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TEST/output.txt", # WILL BREAK
            "-I",
            # "/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/data/", # WILL BREAK 
            # "/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/data/2.txt", # WILL BREAK 
            "/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/data/240711_9913_1kbulls_ars1.2.samples.csv",
            "--dry-run",
            "--debug",
            # "--overwrite",
            "--reference",
            # "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/REF_GENOME_COPY/ARS-UCD1.2.fai", # WILL BREAK!
            "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/REF_GENOME_COPY/ARS-UCD1.2_Btau5.0.1Y",
            ### NO --allele-freq with default ckpt will break!
            "--allele-freq", 
            "/mnt/pixstor/schnabelr-drii/WORKING/jakth2/TRIOS_220704/POPVCF/UMAG1.POP.FREQ.vcf.gz",
            
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
    
    # Check generic command-line flags
    run.check_args()
    
    # Check custom command line flags 
    try:
        # Make the flag [REQUIRED]
        assert (
            run.args.modules
        ), f"missing [REQUIRED] flag: --modules; Please provide the absolute path to an existing modules BASH file containing HPC-cluster-specific software dependencies."
             
        # Confirm path provide is valid
        assert Path(
            run.args.modules
        ).is_file(), f"unable to find the modules file | '{run.args.modules}'"
        
        # Make the flag [REQUIRED] 
        assert (
            run.args.resource_config
        ), "missing [REQUIRED] flag: --resources; Please provide the absolute path to an existing JSON file containing compute resources for pipeline."
        
        # Confirm path provide is valid 
        assert Path(
            run.args.resource_config
        ).is_file(), f"unable to find the resource config JSON file | '{run.args.resource_config}'"
        
        # Make the flag [REQUIRED] 
        assert (
            run.args.ref_file
        ), "missing [REQUIRED] flag: --reference; Please provide the absolute path to an existing .FASTA.FAI file for the reference genome."
        
        # Confirm ref prefix provided points to existing file(s)
        _resolved_ref_path = Path(run.args.ref_file).resolve()
        
        _reference_files = iterdir_with_prefix(
            absolute_path=_resolved_ref_path.parent,
            prefix=_resolved_ref_path.name,
            valid_suffixes=[".fasta", ".fa", ".fai", ".dict"]
            )
        
        assert (len(_reference_files) > 2), f"unable to find the minimum number of reference genome file(s) | '{run.args.ref_file}'"
        
        # Convert a comma-separated string into a python list 
        if "," in run.args.model_prefix:
            _ckpt_list = run.args.model_prefix.split(",")
        else:
            _ckpt_list = [run.args.model_prefix]
        
        # Check for supported variant callers
        _use_deepvariant = partial_match_case_insensitive("deepvariant", _ckpt_list)
        _use_cue = partial_match_case_insensitive("cue", _ckpt_list)
        
        # Confirm at least one supported variant caller was provided
        _no_valid_checkpoint = check_if_all_same([_use_deepvariant, _use_cue], None)
        assert (_no_valid_checkpoint is False), f"unable to find a supported checkpoint (e.g., DeepVariant or Cue) | '{run.args.model_prefix}'"
        
        # Save the list as a new command-line argument
        run.args.model_prefix = [Path(p) for p in _ckpt_list]
        
    except AssertionError as error:
        run.logger.error(f"{error}.\nExiting... ")
        exit(1)
    
    # Initialize all command line inputs
    run.process_args()
    
    # ENTER CUSTOM SUB-MODULES HERE!
    # Handle custom inputs needed for the generic variant calling pipeline
    _cl_inputs = InputManager(
        args=run.args,
        logger=run.logger,
        phase="setup",
    )
    _cl_inputs.update_mode()
    _cl_inputs.create_logging_msg()
     
    # REFERENCE: Confirm a .fai index file exists prior to running PICARD CreateSequenceDict()
    for file in _reference_files:
        if "fai" in file.suffix.lower():
            # Update the command-line args to point to the FASTA file as a Path()
            run.args.ref_file  = Path(file).parent / Path(file).stem
        
    if isinstance(run.args.ref_file, Path) and run.args.ref_file.is_file():
        if _cl_inputs.debug_mode:
            run.logger.debug(f"{_cl_inputs.logger_msg}: valid --reference FASTA file | '{run.args.ref_file}'")
    else:
        _files_found = ",".join(_reference_files)
        run.logger.error(f"{_cl_inputs.logger_msg}: missing a .fai index file in reference genome directory\nFiles found: {_files_found}")
    
    # INPUT PATH: Determine if a file name was given as output, when it should be a directory
    if run._input_path.stem != run._input_path.name:
        # Confirm input is an existing file
        if run._input_path.is_file():
            if _cl_inputs.debug_mode:
                run.logger.debug(f"{_cl_inputs.logger_msg}: valid --input; detected an existing file.")
        else:
            run.logger.error(f"{_cl_inputs.logger_msg}: invalid --input; unable to find a sample CSV file | {run._input_path}\nExiting...")
            exit(1)
    else:
        # TO DO: enable providing an input directory (e.g., samples + metadata together)?
        run.logger.error(f"invalid --input; expected the absolute path to a file, did you enter a directory? | {run._input_path}\nExiting...")
        exit(1)

    # OUTPUT PATH: Determine if a file name was given as output, when it should be a directory
    if run._output_path.stem != run._output_path.name:
        run.logger.error(f"invalid --output-path; expected the absolute path to a directory, did you enter a file? | {run._output_path}\nExiting...")
        exit(1)
    else:
        # Create a new directory, if necessary 
        if not run._output_path.exists():
            _cl_inputs.create_a_dir(dir_name=run._output_path)
        else:
            if _cl_inputs.debug_mode:
                run.logger.debug(f"{_cl_inputs.logger_msg}: valid --output-path; detected an existing directory.")
    
    # Save valid command-line inputs
    _cl_inputs._output_path = run._output_path
    _cl_inputs._input_path = run._input_path
                
    # Load in SLURM resource config file
    _cl_inputs.load_slurm_resources()
    # Prepare pipeline-specific inputs
    _pipeline_inputs = PipelineInputManager(cl_inputs=_cl_inputs,
                                            variant_callers=_variant_callers)
    
    
    run.end_module()


# Execute functions created
if __name__ == "__main__":
    __init__()
