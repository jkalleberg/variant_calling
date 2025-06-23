#!/bin/python3
"""
description: contains custom classes for working with SLURM

usage:
    from sbatch import SBATCH, SubmitSBATCH
"""
from dataclasses import dataclass, field
from regex import compile
from typing import Dict, List, Union, TYPE_CHECKING
from time import sleep
from random import random
from subprocess import run, CalledProcessError

if TYPE_CHECKING:
    from helpers.inputs import InputManager
    from pathlib import Path
    from helpers.files import File, TestFile
    from regex import compile

from helpers.files import TestFile
from helpers.utils import check_if_all_same, collect_job_nums, generate_job_id


@dataclass
class SBATCH:
    """
    Create a custom SBATCH class object, which results in an sbatch file.
    """
    
    # required parameters
    cl_inputs: "InputManager"
    command_list: List[str]
    job_file: "File"
    log_dir: "Path"
    # variant_callers: Dict[str, Dict[str, str]]

    # Optional parameters
    handler_status_label: Union[str, None] = None
    all_lines: List[str] = field(default_factory=list, init=False, repr=False)
    
    # Internal, immutable parameters
    _header_lines: List[str] = field(default_factory=list, init=False, repr=False)
    _job_name: Union[str,None] = field(default=None, init=False, repr=False)
    _line_list: List[str] = field(default_factory=list, init=False, repr=False)
    _start_conda: List[str] = field(default_factory=list, init=False, repr=False)
    _start_sbatch: List[str] = field(default_factory=list, init=False, repr=False)
    _num_lines: Union[None, int] = None
    

    def __post_init__(self) -> None:
        
        self._header_lines = ["#!/bin/bash"]

        # self._start_conda = [
        #     "source ${CONDA_BASE}/etc/profile.d/conda.sh",
        #     "conda deactivate",
        # ]
        
        if "modules" in self.cl_inputs.args:
            if self.cl_inputs.debug_mode:
                self.cl_inputs.logger.debug(
                    f"{self.cl_inputs.logger_msg}: cluster-specific software dependencies added | '{self.cl_inputs.args.modules}'"
                )
            test_modules = TestFile(
                file=self.cl_inputs.args.modules,
                logger=self.cl_inputs.logger,
                )
            test_modules.check_existing()

            if not test_modules.file_exists:
                self.cl_inputs.logger.error(
                    f"{self.cl_inputs.logger_msg}: module file provide does not exist | '{self.cl_inputs.args.modules}'\nExiting..."
                )
                exit(1)

            self._start_sbatch = [
                f". {self.cl_inputs.args.modules}",
            ]
        
        if self._start_sbatch:
            self._start_sbatch.append("echo '=== Science Starts Now: '$(date '+%Y-%m-%d %H:%M:%S')")
        else:
            self._start_sbatch = ["echo '=== Science Starts Now: '$(date '+%Y-%m-%d %H:%M:%S')"]
        
        self._job_name = self.job_file.path.stem

    def create_slurm_headers(self, use_job_array=False) -> None:
        """
        Defines each SLURM flag and writes them in SBATCH header format.

        Usage: slurm_headers(
              partition="BioCompute,Lewis",
              nodes=1,
              ntasks=24,
              mem="30G",
              email="jakth2@mail.missouri.edu"
              )
        """
        if self.cl_inputs.debug_mode:
            self.cl_inputs.logger.debug(f"{self.cl_inputs.logger_msg}: building SBATCH header lines...")
        for key, value in self.cl_inputs.resource_dict.items():
            if key == "email":
                if value != "":
                    self._header_lines.append(f"#SBATCH --mail-user={value}")
                    self._header_lines.append("#SBATCH --mail-type=FAIL")
            elif key == "CPUmem":
                self._header_lines.append("#SBATCH --ntasks-per-core=1")
                self._header_lines.append(f"#SBATCH --mem-per-cpu={str(value)}")
            elif key == "ntasks":
                self._header_lines.append(f"#SBATCH -n {str(value)}")
            elif key == "mem":
                if str(value).isnumeric() and int(value) == 0:
                    self._header_lines.append(f"#SBATCH --{key}={str(value)}")
                    self._header_lines.append(f"#SBATCH --exclusive")
                else:
                    self._header_lines.append(f"#SBATCH --{key}={str(value)}")
            else:
                self._header_lines.append(f"#SBATCH --{key}={str(value)}")

        self._header_lines.append(f"#SBATCH --job-name={self._job_name}")
        
        if use_job_array:
            print("UPDATE SOURCE CODE FOR SLURM JOB ARRAYS!")
            breakpoint()
            # self.header_lines.append(f"#SBATCH --array=1-{self.n_array_tasks}")
            self.header_lines.append(f"#SBATCH --output={self.log_dir}/%x-%A-%a_%j.out")
        else:
            self._header_lines.append(f"#SBATCH --output={self.log_dir}/%x_%j.out")        

        self._header_lines.append("echo '=== SBATCH running on: '$(hostname)")
        self._header_lines.append("echo '=== SBATCH running directory: '${PWD}")
        self._header_lines.extend(self._start_sbatch)
        
        # Review the newly created BASH command(s)
        if self.cl_inputs.debug_mode:
            self.cl_inputs.logger.debug(
                f"{self.cl_inputs.logger_msg}: SBATCH HEADERS: -----------------------------------")
            for line in self._header_lines:
                self.cl_inputs.logger.debug(
                f"{self.cl_inputs.logger_msg}: {line}")
            self.cl_inputs.logger.debug(
                f"{self.cl_inputs.logger_msg}: ---------------------------------------------------")
            breakpoint()

    def update_content_list(self, new_content: List[str]) -> None:
        """
        Edit or copy the content of a SBATCH script file, given a list of Bash commands.

        Args:
            new_list (List[str]): each item represents a single, executable Linux/Bash command.
        """
        if new_content == self.command_list and self.cl_inputs.debug_mode:
           self.cl_inputs.logger.debug(f"{self.cl_inputs.logger_msg}: copying SBATCH contents from Science()...") 
        elif new_content != self.command_list and self.cl_inputs.debug_mode:
            self.cl_inputs.logger.debug(f"{self.cl_inputs.logger_msg}: modifying SBATCH contents from Science()...")
        
        self._line_list.extend(new_content)

    def handle_subprocess_status(
        self, 
        message: Union[str, None] = None,
        status_tracker_file: Union["TestFile", None] = None,
        line_list_index: int = -1
    ) -> None:
        """
        Enables capturing the exit code from a sub-process within a SLURM job file.
        
        For example, if the sub-process fails (exit != 0), this error handler will capture and return the correct exit code.
        The SLURM job script will have the same exit code, allowing SLURM to send the expected job failure email.
        
        Adding this to a specific line within the contents of a SLURM job will prevent running sub-process lines (after line_list_index) if the preceding sub-process fails.

        Args:
            message (Union[str, None], optional): a descriptive label of the sub-process. Defaults to None.
            status_tracker_file (Union[&quot;TestFile&quot;, None], optional): a TestFile() object controlling where to write the status message. Defaults to None.
            line_list_index (int, optional): select a specific line in the SBATCH content that must have a status check. Defaults to -1 (final line).
        """
        if self.cl_inputs.debug_mode:
            self.cl_inputs.logger.debug(f"{self.cl_inputs.logger_msg}: adding status handler...")
        
        original_line = self._line_list[line_list_index]
        
        # Edit Bash function arguments based on inputs to the Python function
        if message is not None and status_tracker_file is not None:
            status_handler = f' && capture_status "{message}" {status_tracker_file.file} || capture_status "{message}" {status_tracker_file.file}'
        elif message is not None and status_tracker_file is None:
            status_handler = f' && capture_status "{message}" || capture_status "{message}"' 
        else:
            status_handler = f' && capture_status || capture_status'
        
        if line_list_index != -1:
            status_handler = status_handler + " &"
        
        # Update the line to include the capture_status() bash function as a suffix
        error_handler = original_line + status_handler
        
        # Revise the SBATCH content lines to include the custom status handler function
        self._line_list[line_list_index] = error_handler

    def create_slurm_job(
        self,
        content_list: Union[None, List[str]] = None,
        handler_status_label: Union[None, str] = None,
        content_index: int = -1,
        # overwrite: bool = False,
    ) -> None:
        """
        Combine SLURM SBATCH header lines, with Linux/Bash command(s) for execution after job submission to SLURM queue.

        Args:
            content_list (List[str]): each item represents a single, executable Linux/Bash command.
            handler_status_label (Union[str, None], optional): a descriptive label used to ensure SLURM job status reflects command status. Defaults to None.
        """
        if len(self._header_lines) == 1:
            self.create_slurm_headers()
        
        # Save whatever lines are entered
        if content_list is not None:
            # Edit the list to include new line(s)
            self.update_content_list(new_content=content_list)
        else:
            # Create a duplicate list
            # NOTE: required to revise with status handling
            self.update_content_list(new_content=self.command_list)
            
        # Force status handler to expect a descriptive message about the subprocess where success is mandatory
        if handler_status_label is not None:
            self.handle_subprocess_status(
                message=handler_status_label,
                # status_tracking_file=self._tracking_file,
                line_list_index=content_index,
            )
        
        # Combine the header lines with the content lines (created with a Science() object)
        if (self.job_file.file_exists and self.cl_inputs.overwrite) or self.job_file.file_exists is False:
            self.all_lines = self._header_lines + self._line_list
            self._num_lines = len(self._line_list)
        elif self.job_file_exists and self.cl_inputs.overwrite is False:
            self._num_lines = None       

    def display_job(self) -> None:
        """
        Prints the SLURM job file contents to the screen.
        """
        if self._num_lines is not None:
            self.cl_inputs.logger.info(
                f"{self.cl_inputs.logger_msg}: file contents for '{self.job_file.path}'\n-------------------------------------"
            )
            print(*self.all_lines, sep="\n")
            print("------------------------------------")

    def write_job(self) -> None:
        """
        Writes the SLURM job file contents to a text file.
        """
        if self._num_lines is not None:
            if self.cl_inputs.debug_mode:
                self.cl_inputs.logger.debug(
                    f"{self.cl_inputs.logger_msg}: job file [{self._job_name}] has [{self._num_lines}] lines of content."
                )
            
            # Save to a new file
            self.job_file.write_list(line_list=self.all_lines)
            
            # Confirm a file was created 
            self.job_file.check_status(should_file_exist=True)

@dataclass
class SubmitSBATCH():
    """
    Create a custom SubmitSBATCH object, which submits an SBATCH file to the SLURM queue.
    """
    job_file: "File"
    
    # internal, immutable parameters
    # NOTE: dataclass inheritance means defaults are REQUIRED 
    
    _dependency_command_list: List[str] = field(default_factory=list, init=False, repr=False)
    _job_id: Union[None, str] = field(default=None, init=False, repr=False)
    _any_prior_job: Union[None, str, List[str]] = field(default=None, init=False, repr=False)
    _status: Union[None, int] = field(default=None, init=False, repr=False)
    _submission_cmd: Union[None, List[str]] = field(default=None, init=False, repr=False) 
    
    def __post_init__(self) -> None:
        # Set new internal, immutable parameters with default values
        self._job_num_pattern: "compile" = compile(r"\d+")

    def build_submission_command(
        self,
        prior_jobs: Union[None, List[str], str] = None,
        allow_dep_failure: bool = False,
    ) -> None:
        """
        Creates a 'sbatch <job_file>' subprocess command, depending on if there are job dependencies or not.
        """
        if prior_jobs is None:
            self._submission_cmd = ["sbatch", self.job_file._test_file.file]
        else:
            self._any_prior_job = prior_jobs
            if isinstance(self._any_prior_job, str):
                if allow_dep_failure:
                    self.slurm_dependency = [
                        f"--dependency=afterany:{self._any_prior_job}",
                        "--kill-on-invalid-dep=yes",
                    ]
                else:
                    self.slurm_dependency = [
                        f"--dependency=afterok:{self._any_prior_job}",
                        "--kill-on-invalid-dep=yes",
                    ]
                self._submission_cmd = (
                    ["sbatch"] + self.slurm_dependency + [self.job_file._test_file.file]
                )
            elif isinstance(self._any_prior_job, list):
                no_priors = check_if_all_same(self.prior_job, None)
                if no_priors:
                    self._submission_cmd = ["sbatch", str(self.job_file)]
                else:
                    self.slurm_dependency = collect_job_nums(
                        self.prior_job, allow_dep_failure=allow_dep_failure
                    )
                    self._submission_cmd = (
                        ["sbatch"] + self.slurm_dependency + [self.job_file.file]
                    )

    def display_command(
        self,
        current_job: int = 1,
        total_jobs: int = 1,
    ) -> None:
        """
        Prints the 'sbatch <job_file>' command used to submit to a SLURM queue.
        """
        if self.job_file.cl_inputs.dry_run_mode:
            self.job_file.cl_inputs.logger.info(
                f"{self.job_file.logger_msg}: pretending to submit SLURM job {current_job}-of-{total_jobs} with command:\n{' '.join(self._submission_cmd)}"
            )

        elif self.job_file.cl_inputs.debug_mode:
            self.job_file.cl_inputs.logger.debug(
                f"{self.job_file.logger_msg}: submitting SLURM job {current_job}-of-{total_jobs} with command:\n{' '.join(self._submission_cmd)}"
            )

    def get_status(
        self,
        current_job: int = 1,
        total_jobs: int = 1,
    ) -> None:
        """
        Determines if a SLURM job was submitted correctly.
        """
        # Sleep for <1 second before submission to SLURM queue
        # Give any previous SBATCH job submissions time to finish before submitting another
        sleep(random())
        
        self.display_command()
        
        # Do not actually run the job on dry_run_mode
        if self.job_file.cl_inputs.dry_run_mode:
            self._job_id = generate_job_id()
            return

        try:
            _result = run(self._submission_cmd, capture_output=True, text=True, check=True)
            self.status = _result.returncode
        
        except CalledProcessError as err:
            error_lines = err.stderr.strip().split("\n")
            for l in error_lines:
                self.job_file.cl_inputs.logger.error(f"{self.job_file.logger_msg}: {l}")
            self.job_file.cl_inputs.logger.error(
                f"{self.job_file.logger_msg}: unable to submit SLURM job {current_job}-of-{total_jobs} | '{self.job_file.file_name}'\nExiting... "
            )
            exit(err.returncode)

        if self.status == 0:
            self.job_file.cl_inputs.logger.info(
                f"{self.job_file.logger_msg}: submitted SLURM job {current_job}-of-{total_jobs}"
            )
            match = self.job_num_pattern.search(_result.stdout)
            if match:
                self._job_id = str(match.group())
                if self.job_file.cl_inputs.debug_mode:
                    self.job_file.cl_inputs.logger.debug(
                        f"{self.job_file.logger_msg}: SLURM job id |  {self._job_id}"
                    )
            else:
                self.job_file.cl_inputs.logger.warning(
                    f"{self.job_file.logger_msg}: unable to detect a SLURM job id from queue submission | '{self.job_file.file_name}'... SKIPPING AHEAD",
                )

    def send_to_queue(self) -> None:
        """
        Send a SBATCH file to the SLURM job queue
        """
        self.build_submission_command()
        self.get_status()   
