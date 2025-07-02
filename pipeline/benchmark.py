#!/usr/bin/python3
"""
description: parse the SLURM resources used to complete an SBATCH job and write to a new CSV file.

example:
    python3 ./pipeline/benchmark.py             \\
        --csv-file /path/to/SLURM_jobs.csv      \\
        --dry-run
"""

# Load python libs
from dataclasses import dataclass, field
from typing import Dict, List
from regex import compile
from sys import exit
from subprocess import CalledProcessError
# import pandas as pd

# from datetime import timedelta


from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from genome import Genome
    from helpers.files import File

from helpers.files import File
from helpers.cmd_line import CMD

@dataclass
class Benchmark:
    """
    Define what data to store when benchmarking the TrioTrain Pipeline.
    """

    # required parameters
    genome: "Genome"

    # internal, immutable values
    _jobs: Dict[str, List[str]] = field(default_factory=dict, init=False, repr=False)
    _keep_decimal = compile(r"[^\d.]+")
    _metrics_list_of_dicts: List[Dict[str, str]] = field(default_factory=list, init=False, repr=False)
    _n_jobs: int = field(default=0, init=False, repr=False)
    _resources_used: list = field(default_factory=list, init=False, repr=False)
    _total_hours: int = field(default=0, init=False, repr=False)
    _total_minutes: int = field(default=0, init=False, repr=False)
    _total_seconds: int = field(default=0, init=False, repr=False) 

    # _skipped_jobs: list = field(default_factory=list, init=False, repr=False)
    # _slurm_jobs: Dict = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        self._command_line = CMD(cl_inputs=self.genome.pipeline_inputs.cl_inputs)
        self._digits_only = compile(r"[-+]?(?:\d*\.*\d+)")

    def find_job_logs(self) -> None:
        """
        Look for potential job numbers in log files
        """
        search_pattern = r"call_variants.\w+_\w+\.out"

        self.genome.pipeline_inputs.cl_inputs.check_outputs(
            search_path=self.genome._log_dir,
            match_pattern=search_pattern,
            file_type="SLURM log files",
        )
        if self.genome.pipeline_inputs.cl_inputs._outputs_exist:

            _jobs_dict = dict()
            _job_ids = list()

            for f in self.genome.pipeline_inputs.cl_inputs._unique_files:
                _log_name_contents = f.split(".")
                _job_type = _log_name_contents[0]
                _sample_id, _job_ext = tuple(_log_name_contents[-2].split("_"))

                match = self._digits_only.search(_job_ext)
                if match:
                    if _job_type in _jobs_dict.keys():
                        _og_job_ids = _jobs_dict[_job_type]
                        _updated_job_ids = _og_job_ids + [match.group()]
                        self.genome.pipeline_inputs.cl_inputs.add_to_dict(
                            update_dict=_jobs_dict,
                            new_key=_job_type,
                            new_val=_updated_job_ids,
                            replace_value=True,
                            verbose=True,
                        )
                    else:               
                        _job_ids.append(match.group())
                        self.genome.pipeline_inputs.cl_inputs.add_to_dict(
                            update_dict=_jobs_dict,
                            new_key=_job_type,
                            new_val=_job_ids,
                            replace_value=True,
                            verbose=True,
                        )

            self.genome.pipeline_inputs.cl_inputs.add_to_dict(
                update_dict = self._jobs,
                new_key = _sample_id,
                new_val = _jobs_dict,
                replace_value = False,
                )

            self._n_jobs += len(_job_ids)

    def get_sec(self, time_str: str) -> int:
        """
        Get seconds from D-HH:MM:SS or HH:MM:SS time
        Source: https://stackoverflow.com/questions/6402812/how-to-convert-an-hmmss-time-string-to-seconds-in-python
        """
        time = time_str.split("-")
        if len(time) == 1:
            days = 0
            hms_string = time[0]
        elif len(time) == 2:
            days = int(time[0])
            hms_string = time[1]
        else:
            self.genome.pipeline_inputs.cl_inputs.logger.error(
                f"expected either 'D-HH:MM:SS' or 'HH:MM:SS' format, but {len(time)} inputs were detected | '{time}'... SKIPPING AHEAD"
            )
            return 0
        h, m, s = hms_string.split(":")
        return int(days) * 86400 + int(h) * 3600 + int(m) * 60 + int(s)

    def process_resources(self, chunk_size: int = 10) -> None:
        """
        Iterate through a list of job ids, and use 'sacct' to calculate resources used per COMPLETED job.
        """

        for sample, jobs_info in self._jobs.items():
            # print("SAMPLE:", sample)
            for job_type, jobs in jobs_info.items():

                # print("JOB TYPE:", job_type)
                # breakpoint()

                _itr = 0

                for job_id in jobs:
                    # print("JOB ID:", job_id)
                    _itr += 1
                    try:
                        # Collect the resources used by the current job number
                        # if the job state=COMPLETED and make the output
                        # parsable with '|' but don't include a trailing '|'
                        # Run the BCFtools command as a sub-process
                        _sacct_cmd = ["sacct", f"-j{job_id}", "--state=COMPLETED", "--format=JobID,JobName%50,State,ExitCode,Elapsed,Alloc,CPUTime,MaxRSS,MaxVMSize", "--units=G", "--parsable2",]
                        _result = self._command_line.execute(
                            command_list=_sacct_cmd,
                            type="sacct",
                            interactive_mode=True,
                            keep_output=True,
                        )
                    except CalledProcessError as err:
                        self.genome.pipeline_inputs.cl_inputs.logger.error(f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: resource collection unavailable | SBATCH job id={job_id}",
                        )
                        self.genome.pipeline_inputs.cl_inputs.logger.error(f"{err}\n{err.stderr}\nExiting... ")
                        exit(err.returncode)

                    # parse out the header line
                    metric_names = _result[0].split("|")[:9]

                    # some jobs may have more than 1 child process
                    if len(_result) == 4:
                        # parse out the more informative job name + jobid line
                        core_hours = _result[1].split("|")[:7]

                        # grab the child process(es) memory usage
                        memory = _result[2].split("|")[7:9]

                        _resources_used = core_hours + memory

                    elif len(_result) < 4:
                        self._skipped_jobs.append(job_id)
                        self.genome.pipeline_inputs.cl_inputs.logger.warning(
                            f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: skipping an SBATCH job ('{job_id}') with an invalid output format\n{_result}"
                        )
                        continue
                    else:
                        self.genome.pipeline_inputs.cl_inputs.logger.error(
                            f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: unable to handle {len(_result)} child process(es) yet!\n{_result}",
                        )
                        exit(2)

                    # Convert CPUTime in days into seconds
                    # NOTE: this is NOT wall time, but how much compute is being charged against
                    #       the user's SLURM account
                    CPUTime = _resources_used[6]
                    CPU_seconds = self.get_sec(CPUTime)

                    # Keep a rolling total of CORE HOUR USAGE ------------
                    self._total_seconds += CPU_seconds

                    # Convert the total seconds into minutes
                    add_minute, remainder_s = divmod(self._total_seconds, 60)
                    self._total_minutes += add_minute
                    self._total_seconds = remainder_s

                    # Convert the total minutes into hours
                    add_hour, remainder_m = divmod(self._total_minutes, 60)
                    self._total_hours += add_hour
                    self._total_minutes = remainder_m

                    # Convert the total hours into days
                    add_day, remainder_h = divmod(self._total_hours, 24)
                    self._total_days = add_day

                    # Remove the 'G' from value of memory used
                    # and convert from int to float
                    MaxRSS = float(self._keep_decimal.sub("", _resources_used[7]))
                    _resources_used[7] = str(MaxRSS)

                    # Add the unit back to column name
                    metric_names[7] = "MaxRSS_G"

                    MaxVMSize = float(self._keep_decimal.sub("", _resources_used[8]))
                    _resources_used[8] = str(MaxVMSize)

                    # Add the unit back to column name
                    metric_names[8] = "MaxVMSize_G"

                    # Add the sample ID and job type to the final dictionary
                    metric_names.insert(0, "JobType")
                    _resources_used.insert(0, job_type)

                    metric_names.insert(0, "SampleID")
                    _resources_used.insert(0, sample)

                    # save clean data to a dictionary
                    d = dict(zip(metric_names, _resources_used))
                    self._metrics_list_of_dicts.append(d)

                    # self.column_names = list(metric_names)

                    if _itr % chunk_size == 0:
                        self._core_hours_str = f"{int(self._total_days):,}-{int(remainder_h):,}:{int(self._total_minutes)}:{self._total_seconds}"
                        self.genome.pipeline_inputs.cl_inputs.logger.info(
                            f"running total CORE HOURS after {_itr:,}-of-{int(self._n_jobs):,} jobs | ~{int(self._total_hours):,} CPU hours ({self._core_hours_str})"
                        )
                        if self.genome.pipeline_inputs.cl_inputs.debug_mode:
                            self.genome.pipeline_inputs.cl_inputs.logger.debug(f"row{_itr:,} = {d}")

    # def get_timedelta(self, total_seconds: int) -> timedelta:
    #     """
    #     Convert number of seconds into a timedelta format.
    #     """
    #     return timedelta(seconds=total_seconds)

    # def get_timedelta_str(self, tdelta) -> str:
    #     """
    #     Re-formats a '0 days 00:00:00' timedelta object back into a '0-00:00:00' from SLURM. Used internally by summary() only.
    #     """
    #     d = {"days": tdelta.days}
    #     d["hours"], remainder = divmod(tdelta.seconds, 3600)
    #     d["minutes"], d["seconds"] = divmod(remainder, 60)
    #     for key, value in d.items():
    #         if key != "days":
    #             d[key] = str(value).zfill(2)
    #     return f'{d["days"]}-{d["hours"]}:{d["minutes"]}:{d["seconds"]}'

    # def str_mem(self, mem: float) -> str:
    #     """
    #     Re-formats a float memory used back to '00.00G' string.
    #     Used internally by process_resources() and summary().
    #     """
    #     mem_string = f"{round(mem,2)}G"
    #     return mem_string

    # def summary(self) -> None:
    #     """
    #     Calculate summary stats about resourced used for each phase.
    #     """
    #     # convert dict obj to dataframe
    #     df = pd.DataFrame.from_records(
    #         self._metrics_list_of_dicts, columns=self.column_names
    #     )

    #     # Ensure you only average usage across non-replicate jobs
    #     df = df.drop_duplicates(subset=["JobName"])

    #     df.insert(
    #         loc=6,
    #         column="Elapsed_seconds",
    #         value=df[["Elapsed"]].applymap(self.get_sec),
    #     )

    #     # Convert to str to timedelta obj for descriptive stats
    #     df.insert(
    #         loc=7,
    #         column="Elapsed_Time",
    #         value=df[["Elapsed_seconds"]].applymap(self.get_timedelta),
    #     )

    #     # Convert to str to float obj for descriptive stats
    #     df[["MaxRSS_G", "MaxVMSize_G"]] = df[["MaxRSS_G", "MaxVMSize_G"]].apply(
    #         pd.to_numeric
    #     )

    #     if self.genome.pipeline_inputs.cl_inputs.debug_mode:
    #         self.genome.pipeline_inputs.cl_inputs.logger.debug(f"accounting output for all jobs |'")
    #         print(
    #             "---------------------------------------------------------------------------------------------------------------------------------------"
    #         )
    #         print(df)
    #         print(
    #             "---------------------------------------------------------------------------------------------------------------------------------------"
    #         )

    #     # handle elapsed wall time
    #     duration_summary = pd.DataFrame(
    #         df.groupby("phase").Elapsed_Time.describe(datetime_is_numeric=False)[
    #             ["count", "mean", "max"]
    #         ]
    #     )
    #     duration_summary[["mean", "max"]] = duration_summary[
    #         ["mean", "max"]
    #     ].applymap(self.get_timedelta_str)

    #     duration_summary.reindex(self.indexes)

    #     if self.genome.pipeline_inputs.cl_inputs.debug_mode:
    #         self.genome.pipeline_inputs.cl_inputs.logger.debug(
    #             f"Duration\n---------------------------------------------\n{duration_summary}\n---------------------------------------------",
    #         )

    #     # handle REAL memory usage
    #     real_mem_summary = pd.DataFrame(
    #         df.groupby("phase").MaxRSS_G.describe()[["count", "mean", "max"]]
    #     )

    #     real_mem_summary[["mean", "max"]] = real_mem_summary[["mean", "max"]].applymap(
    #         self.str_mem
    #     )
    #     real_mem_summary.reindex(self.indexes)

    #     if self.genome.pipeline_inputs.cl_inputs.debug_mode:
    #         self.genome.pipeline_inputs.cl_inputs.logger.debug(
    #             f"Memory Used\n---------------------------------------------\n{real_mem_summary}\n---------------------------------------------"
    #         )

    #     # Merge and clean up the two dfs ------------
    #     # remove duplicate columns to avoid 2 count columns with the same value
    #     counts = list(duration_summary["count"])
    #     duration_summary.drop("count", inplace=True, axis=1)
    #     real_mem_summary.drop("count", inplace=True, axis=1)

    #     # join 2 dataframes
    #     self._merged_df = duration_summary.join(
    #         real_mem_summary, lsuffix="_runtime", rsuffix="_mem"
    #     )

    #     # add back the job count column
    #     self._merged_df.insert(0, "job_count", counts)

    #     # keep the rows in run_order
    #     self._merged_df.reindex(self.indexes)

    #     # add the phase core hours
    #     self._merged_df.loc[list(self._phase_core_hours), "core_hours"] = pd.Series(
    #         self._phase_core_hours
    #     )

    #     # Create a column with phases, rather than row names
    #     self._merged_df.reset_index(inplace=True)

    #     self.genome.pipeline_inputs.cl_inputs.logger.info(
    #         f"Resources Used per phase\n===============================\n{self._merged_df}\n===============================",
    #     )
    #     self.genome.pipeline_inputs.cl_inputs.logger.info(
    #         f"Finished all {int(self._n_jobs):,} jobs\n======= {int(self._n_jobs - len(self._skipped_jobs)):,}-of-{int(self._n_jobs):,} JOBS =======\nTOTAL CORE HOURS = {int(self._total_hours):,} | {self._core_hours_str}\n===============================",
    #     )

    #     if len(self._skipped_jobs) > 0:
    #         self.genome.pipeline_inputs.cl_inputs.logger.warning(
    #             f"{len(self._skipped_jobs)} SLURM jobs were not included in the CPUTime total. Their job numbers are:\n{self._skipped_jobs}",
    #         )

    # def write_results(self) -> None:
    #     """
    #     If dryrun mode, display the intermediate outputs to the screen.

    #     Otherwise, write the intermediate outputs to files.
    #     """
    #     if self.genome.pipeline_inputs.cl_inputs.dry_run_mode:
    #         self.genome.pipeline_inputs.cl_inputs.logger.info(
    #             f"Contents of '{str(self._results_dir)}/{self.name}.summary_resources.csv' |\n---------------------------------------------",
    #         )
    #         print(self._merged_df)
    #         print("---------------------------------------------")
    #     else:
    #         # Define the summary output CSV file to be created
    #         summary_file = File(
    #             self._results_dir / f"{self.name}.summary_resources.csv",
    #             self.genome.pipeline_inputs.cl_inputs.logger,
    #         )
    #         summary_file.check_status()
    #         if summary_file.file_exists:
    #             if self.genome.pipeline_inputs.cl_inputs.debug_mode:
    #                 self.genome.pipeline_inputs.cl_inputs.logger.debug(f"{summary_file.file_name} written")
    #         else:
    #             self._merged_df.to_csv(summary_file.path_to_file, index=False)
    #             assert (
    #                 summary_file.path.exists()
    #             ), f"{summary_file.file_name} was not written correctly"
    #             if self.genome.pipeline_inputs.cl_inputs.debug_mode:
    #                 self.genome.pipeline_inputs.cl_inputs.logger.debug(f"{summary_file.file_name} written")

    def run(self) -> None:
        """
        Combine the benchmark class into a single, callable function.
        """
        self.find_job_logs()
        self.process_resources()
        breakpoint()
        # WHERE I LEFT OFF...
        # TO DO: make a CSV file for COMPLETED SBATCH job resource usage PER SAMPLE
        # TO DO: add each line to a CSV for ALL SAMPLES in pipeline, and then use summary()?
        
        # self.summary()
        # self.write_results()
