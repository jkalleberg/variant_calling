#!/usr/bin/python3
"""
description: define a single unit for the generic variant calling pipeline

example usage: from pipeline.genome import Genome

"""
from dataclasses import dataclass, field
from pathlib import Path
from sys import path, exit
from typing import Dict, List, Union, TYPE_CHECKING
# from os import sched_getaffinity
# from sys import exit

if TYPE_CHECKING:   
    from input import PipelineInputManager
    from helpers.files import File

abs_path = Path(__file__).resolve()
module_path = str(abs_path.parent.parent)
path.append(module_path)

from helpers.utils import count_digits
from helpers.files import File, TestFile
from pipeline.science import Science
from helpers.sbatch import SBATCH
from helpers.sbatch import SubmitSBATCH

# from pipeline.postprocess_vcf import PostProcessVCF

@dataclass
class Genome:
    """
    Contains the per-sample input files to give to a generic variant calling pipeline.
    """

    # required parameters
    sample: tuple
    pipeline_inputs: "PipelineInputManager"

    # optional parameters:
    group_name: Union[str, None] = None
    # trio_order: List[str] = field(default_factory=list, init=True)

    # internal parameters
    # _default_output: Union[None, "File"] = field(default=None, init=False, repr=False)
    # _missing_output: bool = field(default=True, init=False, repr=False)
    _model_type: Union[str, None] = field(default="DeepVariant", init=False, repr=False)
    _pickle_file: Union[None, File] = field(default=None, init=False, repr=False)
    _reads_path: Union[Path, None] = field(default=None, init=False, repr=False)
    _variables: Dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _new_lines: List[str] = field(default_factory=list, init=False, repr=False)
    
    # _check_dict: Dict[str, str] = field(default_factory=dict, init=False, repr=False)
    # # _chrom: Union[str, None] = field(default=None, init=False, repr=False)
    # _data_dict: Dict[str, Union[str, int, List[str]]] = field(
    #     default_factory=dict, init=False, repr=False
    # )
    # _final_genome: bool = field(default=False, init=False, repr=False)
    # _job_ids: List[Union[str, None]] = field(
    #     default_factory=list, init=False, repr=False
    # )
    # _jobid_found: Union[List[bool], None] = field(default=None, init=False, repr=False)
    
    # _model_dict: Dict[str, Union[str, int, List[str]]] = field(
    #     default_factory=dict, init=False, repr=False
    # )
    
    # _paths_found: List[Union[str, None]] = field(
    #     default_factory=list, init=False, repr=False
    # )
    
    # _region: Union[None, str] = field(default=None, init=False, repr=False)
    # _skip_counter: int = field(default=0, init=False, repr=False)
    # _submitting_jobs: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        
        self._sample_num = str(self.sample[0]).zfill(count_digits(self.pipeline_inputs._total_num_genomes))
        self._sample_id = self.sample[1][0]
        
        if self._sample_id is None:
            self._log_msg = f"{self.pipeline_inputs.cl_inputs.logger_msg}"
            _info = "UH OH!"
            print(f"INFO: {_info}")
            breakpoint()
        elif self.sample[1][1] is not None:
            self._reads_path = Path(self.sample[1][1])
            self._resources = self.pipeline_inputs.cl_inputs.resource_dict
            _info = "samples"
        else:
            _info = "trios"
        
        self._log_msg = f"{self.pipeline_inputs.cl_inputs.logger_msg} - [{self._sample_num}-of-{self.pipeline_inputs._total_num_genomes} {_info}]"
    
    def set_outputs(self, verbose: bool = False, format: str = "vcf") -> None:
        """
        Defines the expected file(s).
        """
        if "deepvariant" in self._model_type.lower():
            if "g." in format:
                _extension = "g.vcf.gz"
            else:
                _extension = "vcf.gz"
            
            _output = self._sample_dir / f"{self._sample_id}.{_extension}"
            
        elif "cue" in self._model_type.lower():
            _extension = "vcf"
            _output = self._reports_dir / f"svs.{_extension}"
        else:
            print("WHY DO YOU WANT TO USE A DIFFERENT MODEL TYPE?!?")
            breakpoint()

        _default_output = File(
            path_to_file=_output,
            cl_inputs=self.pipeline_inputs.cl_inputs,
        )
        if self.pipeline_inputs.cl_inputs.overwrite:
            _default_output.check_status(should_file_exist=True)
        else:
            _default_output.check_status()

        if _default_output.file_exists:
            # Uncomment if adding the "check_outputs" flag
            # # If only "checking outputs", skip the remaining steps
            # if (
            #     "check_outputs" in self.pipeline_inputs.cl_inputs.args
            #     and self.pipeline_inputs.cl_inputs.args.check_outputs
            # ):
            #     return
            
            if verbose or self.pipeline_inputs.cl_inputs.debug_mode:
                self.pipeline_inputs.cl_inputs.logger.debug(
                    f"{self._log_msg} - [run_{self._model_type}]: found the default {_extension.upper()} file | '{_default_output.file_name}'"
                )
        
        # Uncomment for per-chr parallelization
        # elif (
        #         self.pipeline_inputs.cl_inputs.args.per_chr
        #         and len(self._paths_found) == self._num_chrs
        #     ):
        #         self.pipeline_inputs.cl_inputs.logger.info(
        #             f"{self._log_msg}: missing the 'bcftools concat' output file | '{_default_output.file_name}'"
        #         )
        else:
            self.pipeline_inputs.cl_inputs.logger.info(
                f"{self._log_msg} - [run_{self._model_type}]: missing the default {_extension.upper()} file | '{_default_output.file_name}'"
            )
        
        # Add the variant-caller-specific default output File()
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self.pipeline_inputs.variant_callers[self._model_type],
            new_key="default_output",
            new_val=_default_output,
            replace_value=True,
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]"
        )
    
    def get_sample_info(self) -> None:
        """
        Print info about the current genome.
        """
        if self._reads_path is not None:
            self.pipeline_inputs.cl_inputs.logger.info(
                f"{self._log_msg}: sample_id='{self._sample_id}' | raw_data='{self._reads_path}'"
            )
        
        # Uncomment to enable DeepTrio
        # else:
        #     self.parents = [x for x in self.trio_order if x != self._sample_id]
        #     self.pipeline_inputs.cl_inputs.logger.info(
        #         f"{self._log_msg}: child_id='{self._sample_id}' | parent_ids={self.parents}"
        #     )
    
    def set_paths(self) -> None:
        """
        Define paths to frequently used directories.
        """
        if self._sample_id is None:
            print("UH OH! Spaghetti-Os!")
            breakpoint()
        else:
            # Are we creating sub-groups within the larger group?
            if self.group_name is not None:
                self._results_dir = (
                    Path(self.pipeline_inputs.cl_inputs._output_path)  / "COHORT" / self.group_name
                )
            else:
                self._results_dir = Path(self.pipeline_inputs.cl_inputs._output_path)
            
            # Define output path structure
            self._sample_dir = self._results_dir / self._sample_id
            self._job_dir = self._sample_dir / "jobs"
            self._log_dir = self._sample_dir / "logs"
            self._tmp_dir = self._sample_dir / "tmp"
            
            # Uncomment to enable per-chr parallelization 
            # if (
            #     "per_chr" in self.pipeline_inputs.cl_inputs.args
            #     and self.pipeline_inputs.cl_inputs.args.per_chr
            #     and self._region is not None
            # ):
            #     self._input_dir = self._sample_dir / self._region
            #     self._reports_dir = self._input_dir / "reports"
            #     self._tmp_dir = self._input_dir / "tmp"
            #     self._scratch_dir = self._input_dir / "scratch"
            
            # Ensure output path structure exists
            self.pipeline_inputs.cl_inputs.create_a_dir(self._results_dir, updated_log_msg=self._log_msg)
            self.pipeline_inputs.cl_inputs.create_a_dir(self._sample_dir, updated_log_msg=self._log_msg)
            self.pipeline_inputs.cl_inputs.create_a_dir(self._job_dir, updated_log_msg=self._log_msg)
            self.pipeline_inputs.cl_inputs.create_a_dir(self._log_dir, updated_log_msg=self._log_msg)
            self.pipeline_inputs.cl_inputs.create_a_dir(self._tmp_dir, updated_log_msg=self._log_msg)
            
            if self._model_type == "Cue":
                self._reports_dir = self._sample_dir / "reports"
                self.pipeline_inputs.cl_inputs.create_a_dir(self._reports_dir, updated_log_msg=self._log_msg)
            elif self._model_type == "DeepVariant":
                self._pop_vcf = TestFile(
                    file=Path(self.pipeline_inputs.cl_inputs.args.pop_file).resolve(),
                    logger=self.pipeline_inputs.cl_inputs.logger)
            
                self._pop_vcf.check_existing()
            
                if not self._pop_vcf.file_exists:
                    self.pipeline_inputs.cl_inputs.logger.info(
                        f"{self.pipeline_inputs.cl_inputs.logger_msg}: missing a valid PopVCF file; unable to use the custom bovine-trained checkpoint.\nPlease update --allele-freq to include an existing PopVCF.\nExiting now...",
                    )
                    exit(1)
    
    def setup_variables(self) -> None:
        """
        Establish a model-specific list of variables saved as nested dictionaries.
        """
        # Create an initial sub-dictionary for a single variant caller
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables,
            new_key=self._model_type,
            new_val=dict(),
            # replace_value=True,
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
            ) 
        # --------------------------------------------------  
        # Reference Genome 
        # -------------------------------------------------- 
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="ref_path",
            new_val=str(self.pipeline_inputs.cl_inputs.args.ref_file.parent),
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
        
        # Add checkpoint prefix to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="ref_name",
            new_val=str(self.pipeline_inputs.cl_inputs.args.ref_file.name),
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
        
        # --------------------------------------------------  
        # Reference Genome - default regions BED file
        # -------------------------------------------------- 
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="region_path",
            new_val=str(self.pipeline_inputs._default_BED_file.path_only),
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
        # Add checkpoint prefix to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="region_name",
            new_val=str(self.pipeline_inputs._default_BED_file.file_name),
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
        
        # -------------------------------------------------- 
        # Model Checkpoint
        # --------------------------------------------------
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="ckpt_path",
            new_val=str(self.pipeline_inputs.variant_callers[self._model_type]["checkpoint_path"]),
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
        # Add checkpoint prefix to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="ckpt_name",
            new_val=self.pipeline_inputs.variant_callers[self._model_type]["checkpoint_name"],
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
        
        # --------------------------------------------------  
        # Reads file (BAM or CRAM input)
        # -------------------------------------------------- 
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="reads_path",
            new_val=str(self._reads_path.parent),
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
        # Add checkpoint prefix to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="reads_name",
            new_val=str(self._reads_path.name),
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
        
        # -------------------------------------------------- 
        # Output VCF (perhaps gVCF in the future)
        # -------------------------------------------------- 
        _default_output = self.pipeline_inputs.variant_callers[self._model_type]["default_output"]
        
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="output_path",
            new_val=str(_default_output.path.parent),
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
        # Add checkpoint prefix to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="output_name",
            new_val=str(_default_output.path.name),
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
        
        # -------------------------------------------------- 
        # Temp Directory
        # -------------------------------------------------- 
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="temp_path",
            new_val=str(self._tmp_dir),
            updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
        )
         
        # -------------------------------------------------- 
        # Population VCF -- no genotypes (DeepVariant Only)
        # --------------------------------------------------
        if self._model_type == "DeepVariant":
            # Add container binding path to model-specific variables
            self.pipeline_inputs.cl_inputs.add_to_dict(
                update_dict=self._variables[self._model_type],
                new_key="pop_path",
                new_val=str(self._pop_vcf.path.parent),
                updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
            )
            # Add checkpoint prefix to model-specific variables
            self.pipeline_inputs.cl_inputs.add_to_dict(
                update_dict=self._variables[self._model_type],
                new_key="pop_name",
                new_val=self._pop_vcf.path.name,
                updated_log_msg=f"{self._log_msg} - [run_{self._model_type}]",
            )
    
    def init_genome(self) -> None:
        """
        Setup a 'Genome()' object.
        """
        self.get_sample_info()
        
        if len(self.pipeline_inputs.variant_callers.keys()) == 1:
            self._model_type = list(self.pipeline_inputs.variant_callers.keys())[0]
            
            self.set_paths()
                        
            if self._sample_id is not None:
                self.set_outputs(verbose=True)
                
                # Uncomment to use g.vcfs (with DeepVariant only)
                # self.set_outputs(verbose=True, format="g.vcf")
                
                _default_output = self.pipeline_inputs.variant_callers[self._model_type]["default_output"]

                # self._outputs = PostProcessVCF(genome=self)
                # self._outputs.check_all_outputs(group_name=self.group_name, verbose=True)
            
                if (
                    _default_output.file_exists is False
                    # self._missing_output
                    # or self._outputs._missing_any_outputs
                    or self.pipeline_inputs.cl_inputs.overwrite
                    ):
                        if "deepvariant" in self._model_type.lower():
                            self.setup_variables()
                        
                        # Uncomment for happy, or per-chr parallelization
                        # self.create_extra_dirs()
                        
                        if "cue" in self._model_type.lower() and self._region is None:
                            self.add_symlinks()
                        return
                else:
                    print("ALL OUTPUTS DETECTED!")
                    breakpoint()
            else:
                print("HELP ME, I'M BROKEN")
                breakpoint()
                # self._outputs = PostProcessVCF(genome=self)
        else:
            print("EDIT TO ENABLE MULTIPLE VARIANT CALLERS")
            breakpoint()
    
    def check_pickle(self, input: File) -> None:
        """
        Confirms if a Genome() object was successfully pickled.

        Args:
            input (File): _description_

        Raises:
            FileNotFoundError: _description_
        """
        self._pickle_file = input
        # Only report a missing file outside of dry-run, where a file won't be written to disk yet
        if self.pipeline_inputs.cl_inputs.dry_run_mode and not self.pipeline_inputs.cl_inputs.overwrite:
            self._pickle_file.check_status()
        else: 
            self._pickle_file.check_status(should_file_exist=True)
        if not self._pickle_file.file_exists:
            if not self.pipeline_inputs.cl_inputs.dry_run_mode:
                raise FileNotFoundError(
                    f"missing required file | '{self._pickle_file.file_name}'"
                )
    
    def init_science(self) -> None:
        """
        Setup the executable lines of science within an SBATCH.
        """
        self._science = Science(genome=self)
        
        # Uncomment to enable per-chrom optimization
        # self._science = Science(genome=self, chr_name=self._chrom)
        
        self._science.build_job_name()
        
        _default_output = self.pipeline_inputs.variant_callers[self._model_type]["default_output"]

        if (
            _default_output.file_exists is False # expect output is missing
            or self._science._job_file.file_exists is False # missing an existing SBATCH
            or self.pipeline_inputs.cl_inputs.overwrite # intending to re-write the SBATCH file
            ):
        
            if "deepvariant" in self._model_type.lower():
                self._science.build_deepvariant_cmd()
                
                # Review the newly created BASH command(s)
                if self.pipeline_inputs.cl_inputs.debug_mode:
                    self.pipeline_inputs.cl_inputs.logger.debug(
                        f"{self._log_msg}: SCIENCE COMMAND: -----------------------------------")
                    for line in self._science._command_list:
                        self.pipeline_inputs.cl_inputs.logger.debug(
                        f"{self._log_msg}: {line}")
                    self.pipeline_inputs.cl_inputs.logger.debug(
                        f"{self._log_msg}: ----------------------------------------------------")
                    breakpoint()
            
            # Uncomment to use Cue
            # if "cue" in self._model_type.lower():
            #     self._science.build_cue_cmd()

            # Add any existing BASH command line commands to the SCIENCE contents
            if self._new_lines:
                self._science.update_command(cmd_list=self._new_lines)
        else:
            self.pipeline_inputs.cl_inputs.logger.warning(
                f"{self._log_msg}: --overwrite=False, skipping variant caller command building | '{self._science._job_file.path}'")
    
    def init_job(self) -> None:
        """
        Setup the SBATCH headers, and combine with content from init_science().
        """
        self._slurm_job = SBATCH(
            cl_inputs=self.pipeline_inputs.cl_inputs,
            command_list=self._science._command_list,
            job_file=self._science._job_file,
            log_dir=self._log_dir,
            )
        
        # Uncomment to by-pass defining variant calling as mandatory
        # self._slurm_job.create_slurm_job()
        
        self._slurm_job.create_slurm_job(handler_status_label=f"variant_calling:{self._model_type}")
        
        # Actually generate the SBATCH file, or pretend to
        if not self.pipeline_inputs.cl_inputs.debug_mode and self.pipeline_inputs.cl_inputs.dry_run_mode:
            self._slurm_job.display_job()
        elif self.pipeline_inputs.cl_inputs.debug_mode and not self.pipeline_inputs.cl_inputs.dry_run_mode:
            self._slurm_job.display_job()
            self._slurm_job.write_job()
        else:
            self._slurm_job.write_job()    
        
    def submit_job(
        self,
        prior_jobs: Union[List[Union[str, None]], None] = None
    ) -> Union[None, str]:
        """
        Pass a SBATCH job to the SLURM queue.
        """
        
        _default_output = self.pipeline_inputs.variant_callers[self._model_type]["default_output"]
        
        if not self.pipeline_inputs.cl_inputs.dry_run_mode:
            # confirm an SBATCH file actually exists
            self._science._job_file.check_status()
        
        if (
            _default_output.file_exists is False
            and self._science._job_file.file_exists
        ) or (
            _default_output.file_exists 
            and self.pipeline_inputs.cl_inputs.overwrite  
        ):
        
            _submit = SubmitSBATCH(
                job_file=self._slurm_job.job_file,
                )
            
            if prior_jobs is not None:
                _submit.build_submission_command(
                    prior_jobs=prior_jobs,
                    allow_dep_failure=True,
                    # allow_dep_failure=False, # Uncomment to trigger successive Bash commands within a single SBATCh file
                    )
                _submit.get_status()
            else:
                _submit.send_to_queue()
            
            return _submit._job_id
        else:
            print("NO SLURM JOB WILL BE SUBMITTED")
            print("OUTPUT FILE EXISTS:", _default_output.file_exists)
            print("JOB FILE EXISTS:", self._science._job_file.file_exists)
        
            print("ANY LINES CREATED?", self._science._n_lines is not None)
            print("OVERWRITE?", self.pipeline_inputs.cl_inputs.overwrite)
            breakpoint()
     
    # def update_logging(self) -> None:
    #     if self._reads_path is None:
    #         self._log_msg = f"{self.pipeline_inputs.cl_inputs.logger_msg} - [{self._sample_num}-of-{self.pipeline_inputs._total_num_genomes} trios]"
    #     else:
    #         self._log_msg = f"{self.pipeline_inputs.cl_inputs.logger_msg} - [{self._sample_num}-of-{self.pipeline_inputs._total_num_genomes} samples]"

    # def create_extra_dirs(self) -> None:
    #     """ """
        # Uncomment for hap.py
        # self.pipeline_inputs.cl_inputs.create_a_dir(self._scratch_dir)
        
        # Uncomment for per-chr parallelization
        # if (
        #     "per_chr" in self.pipeline_inputs.cl_inputs.args
        #     and self.pipeline_inputs.cl_inputs.args.per_chr
        #     and self._region is not None
        # ):
        #     self.pipeline_inputs.cl_inputs.create_a_dir(self._input_dir)
    
    # Uncomment for Cue
    # def add_symlinks(self) -> None:
    #     """
    #     Resolves any issues with 'read-only' permissions with raw data.

    #     Bypasses Cue's hard-coded behavior that writes per-chr input files within the same directory as the original BAM file, and instead write them to the sym link directory.
    #     """
    #     if self._reads_path is None:
    #         return

    #     if "bam" in Path(self._reads_path).suffix:
    #         file_type = "BAM"
    #         index = ".bai"
    #     elif "cram" in Path(self._reads_path).suffix:
    #         file_type = "CRAM"
    #         index = ".crai"
    #     else:
    #         self.pipeline_inputs.cl_inputs.logger.error(
    #             f"{self._log_msg}: unexpected input file format... unable to determine appropriate file extension for the corresponding index file for | '{self._reads_path}'\nExiting..."
    #         )
    #         exit(1)

    #     _reads_path = Path(self._reads_path)
    #     _bam_index_file = Path(f"{self._reads_path}{index}")

    #     if _bam_index_file.is_file():
    #         _bam_symlink_path = self._tmp_dir / _reads_path.name
    #         self.create_sym_link(input=_reads_path, output=_bam_symlink_path)
    #         self._reads_path = str(_bam_symlink_path)

    #         _index_symlink_path = self._tmp_dir / _bam_index_file.name
    #         self.create_sym_link(input=_bam_index_file, output=_index_symlink_path)
    #     else:
    #         self.pipeline_inputs.cl_inputs.logger.error(
    #             f"{self._log_msg}: missing the {file_type} index file | '{_bam_index_file}'\nExiting..."
    #         )
    #         exit(1)
    
    # Uncomment for Cue
    # def create_sym_link(self, input: Path, output: Path) -> None:
    #     """
    #     Transform an existing file into a symbolic link.

    #     Args:
    #         input (Path): existing file
    #         output (str): new symbolic link
    #     """
    #     if not output.exists() or (output.exists() and self.pipeline_inputs.cl_inputs.overwrite):
    #         if self._dryrun_mode:
    #             self.pipeline_inputs.cl_inputs.logger.info(
    #                 f"{self._log_msg}: pretending to create symbolic link | '{str(output)}'='{str(input)}'"
    #             )
    #         else:
    #             if self._debug_mode:
    #                 self.pipeline_inputs.cl_inputs.logger.debug(
    #                     f"{self._log_msg}: creating a symbolic link | '{str(output)}'='{str(input)}'"
    #                 )
    #             if output.exists():
    #                 output.unlink()
    #             output.symlink_to(input)

    
    # def save_new_inputs(self) -> None:
    #     """
    #     Saves bam_path' from an input CSV and writes to a new input CSV for re-running difficult samples.
    #     """
    #     if self._reads_path is None:
    #         return
    #     add_to(
    #         update_dict=self._check_dict,  # type: ignore
    #         new_key="bam_path",
    #         new_val=self._reads_path,
    #         inputs=self.pipeline_inputs.cl_inputs,
    #     )

    #     self.iter._samples_csv.inputs.logger_msg = f"{self._log_msg}"
    #     if self.iter._samples_csv:
    #         self.iter._samples_csv.write_csv(
    #             data_dict=self._check_dict, data_label=self._sample_id
    #         )

    # def setup_per_chr(
    #     self, region: str, region_num: int
    # ) -> Union[List[Union[str, None]], int, None]:
    #     """
    #     Manually perform per-chromosome paralyzation outside of Cue.
    #     """
    #     self._skip_counter = 0
    #     self._itr = str(region_num).zfill(count_digits(self._num_chrs))
    #     self._region = region
    #     self._chr_logger_msg = (
    #         f"{self._log_msg} - [chr {self._itr}-of-{self._num_chrs}]"
    #     )
    #     self._log_msg = self._chr_logger_msg
    #     self.set_outputs()

    #     if self._default_output._file_exists:
    #         self._missing_output = False
    #         self._skip_counter += 1

    #     # the following will be a path to the per-chr default VCF
    #     self._paths_found.append(str(self._default_output.file_name))

    # def set_data(self, chrom: Union[None, str] = None) -> None:
    #     """
    #     Creates the 'data.yaml' file required by Cue.
    #     """
    #     self._chrom = chrom
    #     # USE UPDATE_DICT()?
    #     if self._reads_path is not None:
    #         self._data_dict["bam"] = self._reads_path
    #     self._data_dict["ref"] = str(self.iter.reference)

    #     if self._chrom is None:
    #         self._data_dict["chr_names"] = self.iter._chr_names
    #         # NOTE: 29 AUG 2023 looks weird written to YAML file, but Cue interprets it correctly...
    #         self._output_path = self._sample_dir
    #     else:
    #         if isinstance(self._chrom, str):
    #             self._data_dict["chr_names"] = [self._chrom]
    #         else:
    #             self._data_dict["chr_names"] = self._chrom
    #         self._output_path = Path(self._reports_dir.parent)

    #     self.create_a_dir(self._output_path)

    #     _yaml = File(
    #         path_to_file=self._output_path,
    #         file="data.yaml",
    #         inputs=self.pipeline_inputs.cl_inputs,
    #     )
    #     _yaml._file.check_missing()

    #     if not _yaml._file._file_exists or self.pipeline_inputs.cl_inputs.overwrite:
    #         _yaml.create_config(contents=self._data_dict, data_yaml=True)
    #         _yaml.write_yaml()
    #         if not self.pipeline_inputs.cl_inputs.overwrite:
    #             _yaml._file.check_missing()

    #     if _yaml._file._file_exists or self._dryrun_mode:
    #         self._data_yaml = _yaml.file_path
    #     else:
    #         self.pipeline_inputs.cl_inputs.logger.error(
    #             f"{self._log_msg}: missing a required input file | '{str(_yaml.file_path)}'\nExiting..."
    #         )
    #         exit(1)

    # def set_model(self) -> None:
    #     """
    #     Creates the 'model.yaml' file required by Cue.
    #     """
    #     # USE ADD TO DICT()
    #     self._model_dict["model"] = str(self.iter.model)

    #     # determine which value to use for (nproc)
    #     sbatch_params = ["cpus-per-task", "c", "ntasks", "n"]
    #     _contains_word = lambda s, l: any(map(lambda x: x in s, l))
    #     keys = list(self.iter.resource_dict.keys())
    #     if _contains_word(keys, sbatch_params):
    #         words_found = [x for x in keys if x in sbatch_params]
    #         if words_found:
    #             parameter = words_found[0]
    #             nproc = self.iter.resource_dict[parameter]
    #         else:
    #             nproc = len(sched_getaffinity(0))
    #     else:
    #         nproc = len(sched_getaffinity(0))

    #     # SAVING A COUPLE CORES FOR THE PARENT PROCESS
    #     child_procs = nproc - 2

    #     if (self._num_chrs + 1) <= child_procs:
    #         joblib_tasks = self._num_chrs
    #     else:
    #         # JOBLIB ASSIGNS 2 CORES PER TASK BY DEFAULT,
    #         # SO TO PREVENT OVERLAP OF MULTIPLE CUE JOBS,
    #         # HALVE THE TOTAL CHILD PROCESSES
    #         joblib_tasks = int(round(child_procs / 2))

    #     ## WRITE TO CUE CONFIG FILE
    #     self._model_dict["n_cpus"] = str(joblib_tasks)

    #     _yaml = File(
    #         path_to_file=self._output_path,
    #         file="model.yaml",
    #         inputs=self.pipeline_inputs.cl_inputs,
    #     )
    #     _yaml._file.check_missing()

    #     if not _yaml._file._file_exists or self.pipeline_inputs.cl_inputs.overwrite:
    #         _yaml.create_config(contents=self._model_dict, data_yaml=False)
    #         _yaml.write_yaml()
    #         if not self.pipeline_inputs.cl_inputs.overwrite:
    #             _yaml._file.check_missing()

    #     if _yaml._file._file_exists or self._dryrun_mode:
    #         self._model_yaml = _yaml.file_path
    #     else:
    #         self.pipeline_inputs.cl_inputs.logger.error(
    #             f"{self._log_msg}: missing a required input file | '{str(_yaml.file_path)}'\nExiting..."
    #         )
    #         exit(1)

    # def call_svs(self, region: Union[str, None] = None) -> None:
    #     """
    #     Run Cue to produce SV calls.
    #     """
    #     if not self._default_output._file_exists or (
    #         self.pipeline_inputs.cl_inputs.overwrite and not self.pipeline_inputs.cl_inputs.args.check_outputs
    #     ):
    #         self.set_data(chrom=region)
    #         self.set_model()
    #         self._run_cue = True
    #     else:
    #         self._run_cue = False
    #         self._skip_counter += 1

    # def wrangle_svs(self) -> None:
    #     """
    #     Process the SV calls from Cue into usable format(s).
    #     """
    #     self._submitting_jobs = self._outputs.finalize_vcf()
    #     self._new_lines = self._outputs._command_line._job_cmd

    # def process_svs(self, stream: bool = False) -> None:
    #     if self._pickle_file and (
    #         self._outputs._missing_metrics
    #         or self._outputs._missing_filtered_vcf
    #         or (
    #             self.pipeline_inputs.cl_inputs.overwrite
    #             and not self.pipeline_inputs.cl_inputs.args.check_outputs
    #         )
    #     ):
    #         _summarize_cmd = [
    #             "python3",
    #             "run_cue/summarize_svs.py",
    #             "--genome",
    #             f"{self._pickle_file.file_path}",
    #         ]
    #         if stream:
    #             outputs = self._outputs._command_line.execute(
    #                 command_list=_summarize_cmd,
    #                 type="summarize_svs.py",
    #                 interactive_mode=True,
    #                 keep_output=True,
    #             )
    #             if outputs:
    #                 for line in outputs:
    #                     print(line)
    #         else:
    #             self._outputs._command_line.execute(
    #                 command_list=_summarize_cmd, type="summarize_svs.py"
    #             )
    #             self._new_lines = self._outputs._command_line._job_cmd

    def clean_tmp(self) -> None:
        if self._pickle_file:
            _archive_cmd = [
                "python3",
                "run_cue/archive.py",
                "--genome",
                f"{self._pickle_file.file_path}",
            ]
            self._outputs._command_line.execute(
                command_list=_archive_cmd, type="archive.py"
            )
        self._new_lines = self._outputs._command_line._job_cmd