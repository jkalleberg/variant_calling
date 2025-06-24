#!/usr/bin/python3
"""
description: 

example usage: from pipeline.science import Science

"""
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Union
from os import getcwd, sched_getaffinity
from multiprocessing import cpu_count
import re
from spython.main import Client

if TYPE_CHECKING:
    from genome import Genome

from helpers.files import File

@dataclass
class Science:
    """
    Creates the command line 'science' to be run.
    """

    # required parameters
    genome: "Genome"

    # optional parameters
    chr_name: Union[None, str] = None

    # internal parameters
    _base_binding: str = field(
        default="/usr/lib/locale/:/usr/lib/locale/", init=False, repr=False
    )
    _n_lines: Union[int, None] = field(init=False, repr=False, default=None) 
    _job_name: Union[str, None] = field(init=False, repr=False, default=None)
    _command_list: List[str] = field(init=False, repr=False, default_factory=list)

    def build_job_name(self, prefix: str = "call_variants") -> None:
        """
        Create the SLURM SBATCH file name.
        """
        if self.chr_name:
            self._job_name = f"{prefix}.{self.genome._sample_id}.{self.chr_name}"
        else:
            self._job_name = f"{prefix}.{self.genome._sample_id}"

        self._job_file = File(
            path_to_file = self.genome._job_dir / f"{self._job_name}.sbatch",
            cl_inputs=self.genome.pipeline_inputs.cl_inputs,
        )
        if self.genome.pipeline_inputs.cl_inputs.overwrite:
            self._job_file.check_status(should_file_exist=True)
        else:
            self._job_file.check_status()

    # Uncomment to use Cue
    # def build_cue_cmd(self) -> None:
    #     """
    #     Create a list of lines for a SV-Calling within SLURM SBATCH file.
    #     """
    #     self._command_list = [
    #         "ROOT_DIR=$(dirname $(pwd))",
    #         "export PYTHONPATH=${ROOT_DIR}/cue:${PYTHONPATH}",
    #         f"export XDG_RUNTIME_DIR={self.genome._scratch_dir}",
    #         f"export JOBLIB_TEMP_FOLDER={self.genome._tmp_dir}/",
    #         f"python3 ../cue/engine/call.py --data_config {str(self.genome._data_yaml)} --model_config {str(self.genome._model_yaml)}",
    #     ]
    
    def setup_container(self, type: str = "CPU") -> None:
        """
        Determine which Apptainer container to use.

        NOTE: Much match the hardware requested via SLURM in --resource-config.
        """        
        if self.genome._model_type == "DeepVariant":
            _input_version = self.genome.pipeline_inputs.variant_callers[self.genome._model_type]["version"].split("_")[0]
            
            # Matches any character that is NOT a letter or a space
            # (i.e., it keeps digits and special characters)
            self._version = "".join(re.findall(r'[^a-zA-Z\s]', _input_version))
            
            if type == "GPU":
                self._container = f"{self.genome._model_type.lower()}_{self._version}-gpu.sif"
            else:
                self._container = f"deepvariant_{self._version}.sif"
        elif self.genome._model_type == "DeepTrio":
            if type == "GPU":
                self._container = f"deepvariant_deeptrio-{self._version}-gpu.sif"
            else:
                self._container = f"deepvariant_deeptrio-{self._version}.sif"
        elif self.genome._model_type == "Cue":
            print("SET THE FORMAT FOR CUE'S CONTAINER")
            breakpoint()

        self.genome.pipeline_inputs.cl_inputs.logger.info(f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: using the {type} container | '{self._container}'")
    
    def create_bindings(self) -> None:
        """
        Create the path bindings for Apptainer/Singularity
        """
        bindings = [self._base_binding, f"{getcwd()}/:/run_dir/"]
        
        # Only select the variable PATHs, ignoring the file name(s) for now        
        _variable_paths = {key: value for key, value in self.genome._variables[self.genome._model_type].items() if "path" in key.lower()}
        
        for binding_name, path in _variable_paths.items():
            if f"/{binding_name}/" not in bindings:
                _new_binding = f"{path}/:/{binding_name}/"
                if self.genome.pipeline_inputs.cl_inputs.debug_mode:
                    self.genome.pipeline_inputs.cl_inputs.logger.debug(f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: adding a new container binding | '{_new_binding}'")
                bindings.append(_new_binding)
            else:
                if self.genome.pipeline_inputs.cl_inputs.debug_mode:
                    self.genome.pipeline_inputs.cl_inputs.logger.debug(f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: skipping a duplicate binding | '{_new_binding}'")

        self._bindings_str = ",".join(bindings)
        if self.genome.pipeline_inputs.cl_inputs.debug_mode:
            self.genome.pipeline_inputs.cl_inputs.logger.debug(f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: container bindings | '{self._bindings_str}'")
        
    def setup_slurm_config(self) -> None:
        """
        Confirm the user has provided multiple processing cores in a SLURM config file.
        
        Otherwise, use the current number of cores available to the user.
        """
        if "ntasks" in self.genome.pipeline_inputs.cl_inputs.resource_dict.keys():
            self._nproc = self.genome.pipeline_inputs.cl_inputs.resource_dict["ntasks"]
            _label = "expected"
        else: 
            _label = "available"
            try:
                # This is preferred as it accurately reflects the CPU affinity
                # and thus the number of CPUs available to the current process,
                # especially in environments with CPU restrictions (like cgroups).
                self._nproc  = len(sched_getaffinity(0))
            except AttributeError:
                # Fallback for systems that do not support os.sched_getaffinity,
                # such as macOS or older Python versions.
                # This returns the total number of logical CPUs on the system,
                # which might not reflect process-specific CPU restrictions.
                self._nproc  = cpu_count()

        if self.genome.pipeline_inputs.cl_inputs.debug_mode:
            self.genome.pipeline_inputs.cl_inputs.logger.debug(f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: number of {_label} processing units | '{self._nproc}'")  

    def get_help(self) -> None:
        """
        Display the help page for the program within the container used (make_examples)
        """
        self.setup_container()
        get_help = Client.execute(  # type: ignore
            self._container,
            # Uncomment for DeepTrio
            # ["/opt/deepvariant/bin/deeptrio/run_deeptrio", "--helpfull"],
            ["/opt/deepvariant/bin/run_deepvariant", "--helpfull"],
            bind=[self._base_binding],
        )
        print(get_help["message"])
        # print(get_help["message"][0])
    
    def build_deepvariant_cmd(self,
                              sequence_type: str = "WGS",
                              ) -> None:
        """
        Combine container, bindings, and flags into a single Apptainer command.
        """
        self.setup_container()
        self.create_bindings()
        self.setup_slurm_config() 
        
        flags = [
            f"--model_type={sequence_type}",
            f"--num_shards={self._nproc}",
            f"--sample_name={self.genome._sample_id}",
        ]
        
        # Select only the variable names, ignoring the variable paths
        _variable_names = {key: value for key, value in self.genome._variables[self.genome._model_type].items() if "name" in key.lower()}
        for k,v in _variable_names.items():
            _k_prefix = k.split("_")[0]
            _binding = f"{_k_prefix}_path"
            if "ref" in k:
                flags.append(f"--ref=/{_binding}/{v}")
            elif "region" in k:
                flags.append(f"--regions=/{_binding}/{v}")
            elif "ckpt" in k:
                flags.append(f"--customized_model=/{_binding}/{v}")
            elif "reads" in k:
                flags.append(f"--reads=/{_binding}/{v}")
            elif "output" in k:
                if "g." in v:
                    flags.append(f"--output_gvcf=/{_binding}/{v}")
                else:
                    flags.append(f"--output_vcf=/{_binding}/{v}")
            elif "temp" in k or "tmp" in k:
                flags.append(f"--intermediate_results_dir=/{_binding}/{v}")
            elif "pop" in k:
                flags.append(f'--make_examples_extra_args="use_allele_frequency=true,population_vcfs=/{_binding}/{v}"') 
            else:
                print("UNEXPECTED FLAG NAME FOUND!")
                breakpoint()

        self._flags_str = " ".join(flags)
        
        if self.genome.pipeline_inputs.cl_inputs.debug_mode:
            self.genome.pipeline_inputs.cl_inputs.logger.debug(f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: flags used | '{self._flags_str}'")
            
        _dv_command_list = [
            f'echo "INFO: using {self.genome._model_type} with {self.genome._variables[self.genome._model_type]["ckpt_name"]} to call variants for sample={self.genome._sample_id}"',
            f"time apptainer run -B {self._bindings_str} {self._container} /opt/deepvariant/bin/run_deepvariant {self._flags_str}",
        ]
        self.update_command(cmd_list=_dv_command_list)

    def update_command(self, cmd_list: List[str]) -> None:
        """
        Add additional lines to the SLURM SBATCH file.

        Parameters
        ----------
        cmd_list : List[str]
            a list of new lines to include in the SBATCH file
        """
        if self._command_list:
            self._command_list = self._command_list + cmd_list
        else:
            self._command_list = cmd_list