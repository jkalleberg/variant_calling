#!/usr/bin/python3
"""
description: after run.py, report the resource usage after variant calling.

usage: python3 compute.py                 \
        -I ../CATTLE_TEST/1051/1051.pkl   \
        --dry-run                         \
                    
"""

from pathlib import Path
from sys import path, exit
abs_path = Path(__file__).resolve()
module_path = str(abs_path.parent.parent)
path.append(module_path)

from helpers.module_builder import CustomModule
from helpers.inputs import InputManager
from helpers.files import File
from pipeline.genome import Genome

from pipeline.benchmark import Benchmark


def __init__() -> None:
    _benchmark = CustomModule(output_required=False)
    _benchmark.start_module()

    # Uncomment to force arg entry at command line
    # _benchmark.collect_args()

    # Edit for manually testing command line arguments
    _benchmark.collect_args(
        [
            "-I",
            "../CATTLE_TEST/384425/384425.pkl",
            # "--dry-run",
            # "--debug",
            # "--overwrite",
        ]
    )

    try:
        # Check generic command-line flags
        _benchmark.check_args()

    except AssertionError as error:
        _benchmark._logger.error(f"{error}.\nExiting... ")
        exit(1)

    # Initialize all command line inputs
    _benchmark.process_args()

    # Handle command-line inputs needed
    _cl_inputs = InputManager(
        args=_benchmark._args,
        logger=_benchmark._logger,
        phase="benchmark",
    )
    _cl_inputs.update_mode()
    _cl_inputs.create_logging_msg()

    # INPUT PATH: Determine if a directory name was given as output, when it should be a directory
    if _benchmark._input_path.stem != _benchmark._input_path.name:
        # Confirm input is an existing file
        if _benchmark._input_path.is_file():
            if _cl_inputs.debug_mode:
                _benchmark._logger.debug(
                    f"{_cl_inputs.logger_msg}: valid --input; detected an existing file."
                )
            _cl_inputs._input_path = _benchmark._input_path
        else:
            _benchmark._logger.error(
                f"{_cl_inputs.logger_msg}: invalid --input; unable to find a sample CSV file | {_benchmark._input_path}\nExiting..."
            )
            exit(1)
    else:
        # TO DO: enable providing an input directory (e.g., samples + metadata together)?
        _benchmark._logger.error(
            f"invalid --input; expected a file, did you enter a directory? | {_benchmark._input_path}\nExiting..."
        )
        exit(1)

    # Find the picked data for a specific sample
    _pickle_file = File(
        path_to_file=_benchmark._input_path,
        cl_inputs=_cl_inputs,
    )
    _genome = _pickle_file.load_pickle()

    try:
        # Confirm that a pickled Genome() object was loaded
        assert _genome, "unable to re-open the pickled Genome()"
        assert isinstance(_genome, Genome), "unable to re-open the pickled Genome()"

        # Update the 'mode' based on current script args
        _mode_args = ["debug", "dry_run", "overwrite"]

        # For specific command-line args...
        for arg in _mode_args:
            # Determine if these args have been manually changed by the user
            _current_value = vars(_benchmark._args)[arg]
            _previous_value = vars(_genome.pipeline_inputs.cl_inputs.args)[arg]
            if _previous_value != _current_value:
                setattr(_genome.pipeline_inputs.cl_inputs.args, arg, _current_value)

        # Update the previous 'mode' argument
        _genome.pipeline_inputs.cl_inputs.update_mode()

        # Update the phase to reflect the current step
        _genome.pipeline_inputs.cl_inputs.phase = _cl_inputs.phase
        _genome.pipeline_inputs.cl_inputs.create_logging_msg()
        _genome.pipeline_inputs.cl_inputs.logger = _benchmark._logger

        # Start the post-variant-calling SBATCH resource usage benchmarking
        benchmark_jobs = Benchmark(genome=_genome)

        for (
            k,
            vs,
        ) in _genome.pipeline_inputs.variant_callers.items():  # print(f"KEY: {k}={vs}")
            _default_output = vs["default_output"]

            if _default_output.path.is_file():
                print("DEFAULT OUTPUT EXISTS:")
                benchmark_jobs.find_job_logs()
                benchmark_jobs.process_resources(chunk_size=1)
                breakpoint()
            else:
                print("MISSING DEFAULT OUTPUT")

    except AssertionError as error:
        _benchmark._logger.error(f"{error}.\nExiting... ")
        exit(1)

    _benchmark.end_module()


# Execute functions created
if __name__ == "__main__":
    __init__()
