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
# from pipeline.postprocess_vcf import PostProcessVCF

# from sbatch import SBATCH, SubmitSBATCH
from pipeline.science import Science



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
    # _new_lines: List[str] = field(default_factory=list, init=False, repr=False)
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
            print("HALT!")
            breakpoint()
        elif self.sample[1][1] is not None:
            self._reads_path = Path(self.sample[1][1])
            self._resources = self.pipeline_inputs.cl_inputs.resource_dict
            _info = "samples"
        else:
            _info = "trios"
        
        self._log_msg = f"{self.pipeline_inputs.cl_inputs.logger_msg} - [{self._sample_num}-of-{self.pipeline_inputs._total_num_genomes} {_info}]"
        
        # print(f"SAMPLE #: {self._sample_num}\nSAMPLE ID: {self._sample_id}")
        # print("LOG MSG:", self._log_msg)
        # breakpoint()
    
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
            print("PESKY USER, WHY DO YOU WANT TO USE A DIFFERENT MODEL TYPE?")
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
                    f"{self._log_msg}: found the default {_extension.upper()} file | '{_default_output.file_name}'"
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
                f"{self._log_msg}: missing the default {_extension.upper()} file | '{_default_output.file_name}'"
            )
        
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self.pipeline_inputs.variant_callers[self._model_type],
            new_key="default_output",
            new_val=_default_output,
        )
    
    def get_status(self) -> None:
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
            self.pipeline_inputs.cl_inputs.create_a_dir(self._results_dir)
            self.pipeline_inputs.cl_inputs.create_a_dir(self._sample_dir)
            self.pipeline_inputs.cl_inputs.create_a_dir(self._job_dir)
            self.pipeline_inputs.cl_inputs.create_a_dir(self._log_dir)
            self.pipeline_inputs.cl_inputs.create_a_dir(self._tmp_dir)
            
            if "cue" in self._model_type.lower():
                self._reports_dir = self._sample_dir / "reports"
                self.pipeline_inputs.cl_inputs.create_a_dir(self._reports_dir)
            elif "deepvariant" in self._model_type.lower():
                # Confirm a PopVCF containing allele frequency data was provided by the user
                if self.pipeline_inputs.cl_inputs.args.pop_file is None:
                    self.pipeline_inputs.cl_inputs.logger.info(
                        f"{self.pipeline_inputs.cl_inputs.logger_msg}: invalid --allele-freq; unable to use the custom bovine-trained checkpoint without a PopVCF.\nExiting now...",
                    )
                    exit(1)
                
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
        # Create an initial sub-dictionary for a single variant aller
        if self._model_type not in self._variables.keys():
            self.pipeline_inputs.cl_inputs.add_to_dict(
                update_dict=self._variables,
                new_key=self._model_type,
                new_val=dict(),
            )
        
        # --------------------------------------------------  
        # Reference Genome 
        # -------------------------------------------------- 
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="ref_path",
            new_val=str(self.pipeline_inputs.cl_inputs.args.ref_file.parent),
        )
        # Add checkpoint prefix to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="ref_name",
            new_val=str(self.pipeline_inputs.cl_inputs.args.ref_file.name),
        )
        
        # --------------------------------------------------  
        # Reference Genome - default regions BED file
        # -------------------------------------------------- 
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="region_path",
            new_val=str(self.pipeline_inputs._default_BED_file.path_only),
        )
        # Add checkpoint prefix to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="region_name",
            new_val=str(self.pipeline_inputs._default_BED_file.file_name),
        )
        
        # -------------------------------------------------- 
        # Model Checkpoint
        # --------------------------------------------------
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="ckpt_path",
            new_val=str(self.pipeline_inputs.variant_callers[self._model_type]["checkpoint_path"]),
        )
        # Add checkpoint prefix to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="ckpt_name",
            new_val=self.pipeline_inputs.variant_callers[self._model_type]["checkpoint_name"],
        )
        
        # --------------------------------------------------  
        # Reads file (BAM or CRAM input)
        # -------------------------------------------------- 
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="reads_path",
            new_val=str(self._reads_path.parent),
        )
        # Add checkpoint prefix to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="reads_name",
            new_val=str(self._reads_path.name),
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
        )
        # Add checkpoint prefix to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="output_name",
            new_val=str(_default_output.path.name),
        )
        
        # -------------------------------------------------- 
        # Temp Directory
        # -------------------------------------------------- 
        # Add container binding path to model-specific variables
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self._variables[self._model_type],
            new_key="temp_path",
            new_val=str(self._tmp_dir),
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
            )
            # Add checkpoint prefix to model-specific variables
            self.pipeline_inputs.cl_inputs.add_to_dict(
                update_dict=self._variables[self._model_type],
                new_key="pop_name",
                new_val=self._pop_vcf.path.name,
            )    
    
    def init_genome(self) -> None:
        """
        Setup a 'Genome()' object.
        """
        self.get_status()
        
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
        self._pickle_file.check_status(should_file_exist=True)
        if not self._pickle_file.file_exists:
            if not self.pipeline_inputs.cl_inputs.dry_run_mode:
                raise FileNotFoundError(
                    f"missing required file | '{self._pickle_file.file_name}'"
                )
    
    def init_science(self) -> None:
        """
        Setup the lines of science within an SBATCH.
        """
        self._science = Science(genome=self)
        
        # Uncomment to enable per-chrom optimization
        # self._science = Science(genome=self, chr_name=self._chrom)
        
        self._science.build_job_name()
        
        print("JOB NAME:", self._science._job_name)
        print("JOB FILE:", self._science._jobfile_str)

        if "deepvariant" in self._model_type.lower():
            self._science.build_deepvariant_cmd()
            print("ADD A BUILD DV COMMAND HERE!")
            breakpoint()
        
        # Uncomment to use Cue
        # elif "cue" in self._model_type.lower():
        #     self._science.build_cue_cmd()
        else:
            print("BROKEN")
            breakpoint()

        if self._new_lines:
            self._science.update_command(cmd_list=self._new_lines)
    
