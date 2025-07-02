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
from typing import Dict, List, Union
from regex import compile
from sys import exit
from subprocess import CalledProcessError
import pandas as pd

from datetime import timedelta


from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from genome import Genome
    from helpers.files import File
    from regex import compile
    from helpers.cmd_line import CMD
    from datetime import timedelta

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
    _all_samples_file: Union[None, "File"] = field(default=None, init=False, repr=False)
    _command_line: Union[None,"CMD"] = field(default=None, init=False, repr=False)
    _digits_only: "compile" = compile(r"[-+]?(?:\d*\.*\d+)")
    _group_name: Union[None, str] = field(default=None, init=False, repr=False)
    _jobs: Dict[str, List[str]] = field(default_factory=dict, init=False, repr=False)
    _keep_decimal: "compile" = compile(r"[^\d.]+")
    _metrics_data: pd.DataFrame = field(default_factory=pd.DataFrame, init=False, repr=False) 
    _metrics_list: List[Dict[str, str]] = field(default_factory=list, init=False, repr=False)
    _n_jobs: int = field(default=0, init=False, repr=False)
    _resources_used: list = field(default_factory=list, init=False, repr=False)
    _total_hours: int = field(default=0, init=False, repr=False)
    _total_minutes: int = field(default=0, init=False, repr=False)
    _total_seconds: int = field(default=0, init=False, repr=False)
    _sample_file: Union[None, "File"] = field(default=None, init=False, repr=False)
    _skipped_jobs: List[str] = field(default_factory=list, init=False, repr=False)
    _summary_data: pd.DataFrame = field(
        default_factory=pd.DataFrame, init=False, repr=False
    )
    _summary_file: Union[None,"File"] = field(default=None, init=False, repr=False) 
    # _slurm_jobs: Dict = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        self._command_line = CMD(cl_inputs=self.genome.pipeline_inputs.cl_inputs)

        # Define the single-sample output CSV file to be created
        self._sample_file = File(
            path_to_file=self.genome._sample_dir / "resources_used.csv",
            cl_inputs=self.genome.pipeline_inputs.cl_inputs,
        )
        if self.genome.pipeline_inputs.cl_inputs.overwrite:
            self._sample_file.check_status(should_file_exist=True)
        else:
            self._sample_file.check_status(should_file_exist=False)

        # Define the multi-sample output CSV file to be created
        if self.genome._summary_dir is None:
            self.genome._summary_dir = self.genome._results_dir / "RESULTS"
            self.genome._summary_dir.mkdir(exist_ok=True)
        self._group_name = self.genome.pipeline_inputs.cl_inputs._input_path.stem
        self._all_samples_file = File(
            path_to_file=self.genome._summary_dir / f"{self._group_name}.resources_used.csv",
            cl_inputs=self.genome.pipeline_inputs.cl_inputs,
        )
        if self.genome.pipeline_inputs.cl_inputs.overwrite:
            self._all_samples_file.check_status(should_file_exist=True)
        else:
            self._all_samples_file.check_status(should_file_exist=False)

        # Define the multi-sample output CSV that reports benchmarking stats
        self._summary_file = File(
            path_to_file=self.genome._summary_dir / f"{self._group_name}.resources_stats.csv",
            cl_inputs=self.genome.pipeline_inputs.cl_inputs,
        )
        if self.genome.pipeline_inputs.cl_inputs.overwrite:
            self._summary_file.check_status(should_file_exist=True)
        else:
            self._summary_file.check_status(should_file_exist=False)

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

                        # Ensure that command is executed even if in dry_run_mode
                        # BUT DO NOT TURN OFF FOR OTHER INSTANCES OF CL_INPUTS
                        if self._command_line.cl_inputs.dry_run_mode is True:
                            self._command_line.cl_inputs.dry_run_mode = False

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
                    self._metrics_list.append(d)

                    if _itr % chunk_size == 0:
                        self._core_hours_str = f"{int(self._total_days):,}-{int(remainder_h):,}:{int(self._total_minutes)}:{self._total_seconds}"
                        self.genome.pipeline_inputs.cl_inputs.logger.info(
                            f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: running total CORE HOURS after {_itr:,}-of-{int(self._n_jobs):,} jobs | ~{int(self._total_hours):,} CPU hours ({self._core_hours_str})"
                        )
                        if self.genome.pipeline_inputs.cl_inputs.debug_mode:
                            self.genome.pipeline_inputs.cl_inputs.logger.debug(
                                f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: row{_itr:,} = {d}"
                            )

        # convert list of dicts obj to Pandas dataframe
        self._metrics_data = pd.DataFrame.from_records(
            self._metrics_list, columns=list(metric_names)
        )

        # Exclude metrics for non-replicate jobs
        self._metrics_data.drop_duplicates(subset=["JobName"], inplace=True)

    def write_intermediates(self) -> None:
        """
        If dry_run mode, display the intermediate outputs to the screen.

        Otherwise, write the intermediate outputs to files.
        """
        # Write the contents of the single-sample CSV file
        if self._sample_file.file_exists is False:
            self._sample_file.write_dataframe(df=self._metrics_data,
                                        keep_header=True,
                                        keep_index=False,
                                        delim=",")
            self._sample_file.check_status(should_file_exist=True)
        elif self.genome.pipeline_inputs.cl_inputs.overwrite:
            self.genome.pipeline_inputs.cl_inputs.logger.info(
                f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: --overwrite=True, re-writing the single-sample benchmarking file | '{self._sample_file.path}'"
            )
            self._sample_file.write_dataframe(df=self._metrics_data,
                                        keep_header=True,
                                        keep_index=False,
                                        delim=",")
            self._sample_file.check_status(should_file_exist=True)
        else:
            self.genome.pipeline_inputs.cl_inputs.logger.info(
                f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: found the single-sample benchmarking file | '{self._sample_file.path}'"
            )

        # Write the contents of the multi-sample CSV file
        if self._all_samples_file.file_exists is False:
            self._all_samples_file.write_dataframe(
                df=self._metrics_data, keep_header=True, keep_index=False, delim=","
            )
            self._all_samples_file.check_status(should_file_exist=True)
        elif (self._all_samples_file.file_exists and self.genome.pipeline_inputs.cl_inputs.overwrite):

            self.genome.pipeline_inputs.cl_inputs.logger.info(
                f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: --overwrite=True, re-writing the multi-sample benchmarking file | '{self._all_samples_file.path}'"
            )
            self._all_samples_file.write_dataframe(
                df=self._metrics_data, keep_header=True, keep_index=False, delim=","
            )
            self._all_samples_file.check_status(should_file_exist=True)

        elif self._all_samples_file.file_exists and self.genome.pipeline_inputs.cl_inputs.overwrite is False:

            # load in previous data:
            self._all_samples_file.load_csv()

            # Clear any previously saved data
            _uniq_list = list()

            # Use the previous contents to ensure duplicate data are not written
            for line in self._all_samples_file._existing_data:
                uniq_string = f"{line["SampleID"]}_{line["JobName"]}"
                # new_values = {key: val for key, val in line.items() if key != uniq_string}
                # self._metadata_dict[new_key] = new_values
                _uniq_list.append(uniq_string)

            for row in self._metrics_list:
                if f"{row["SampleID"]}_{row["JobName"]}" in _uniq_list:
                    if self.genome.pipeline_inputs.cl_inputs.debug_mode:
                        self.genome.pipeline_inputs.cl_inputs.logger.debug(
                            f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: skipping duplicate row contents saved previously.\nROW={row}"
                        )
                        breakpoint()
                else:
                    _num_rows = len(self._metrics_list)
                    if _num_rows > 1:
                        msg="new rows"
                    else:
                        msg="a new row"
                    self.genome.pipeline_inputs.cl_inputs.logger.info(
                        f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: --overwrite=False, adding {msg} to the multi-sample benchmarking file | '{self._all_samples_file.path}'"
                    )
                    _col_names = list(self._metrics_list[0].keys())
                    self._all_samples_file.add_row(
                        data_dict=row, col_names=_col_names,
                    )

    def get_timedelta(self, total_seconds: int) -> "timedelta":
        """
        Convert number of seconds into a timedelta format.
        """
        return timedelta(seconds=total_seconds)

    def load_all_samples(self) -> None:
        """
        After benchmarking the COMPLETED SBATCH jobs for multiple Genome() objects,
        load in the multi-sample CSV for summarization.
        """

        if (self._all_samples_file.file_exists and
            (self._summary_file.file_exists is False or 
             self.genome.pipeline_inputs.cl_inputs.overwrite)
            ):

            self._all_samples_file.load_csv()
            self._metrics_list = self._all_samples_file._existing_data

            #  convert list of dicts obj to Pandas dataframe
            self._metrics_data = pd.DataFrame.from_records(
                self._metrics_list, columns=list(self._metrics_list[0].keys())
            )

            # Exclude metrics for non-replicate jobs
            self._metrics_data.drop_duplicates(subset=["JobName"], inplace=True)

            # Convert the DD-HH:MM:SS format string to a total_seconds
            self._metrics_data["Elapsed_seconds"] = self._metrics_data[["Elapsed"]].map(self.get_sec)

            # Convert the DD-HH:MM:SS format string to a timedelta obj for descriptive stats
            self._metrics_data["Elapsed_Time"] = self._metrics_data[["Elapsed_seconds"]].map(
                self.get_timedelta
            )

            # Convert to memory strings to floats for descriptive stats
            self._metrics_data[["MaxRSS_G", "MaxVMSize_G"]] = self._metrics_data[["MaxRSS_G", "MaxVMSize_G"]].apply(
                pd.to_numeric
            )

            if self.genome.pipeline_inputs.cl_inputs.debug_mode:
                self.genome.pipeline_inputs.cl_inputs.logger.debug(f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: accounting output for all jobs |'")
                print(
                    "---------------------------------------------------------------------------------------------------------------------------------------"
                )
                # Print the DataFrame without headers and without the index
                print(self._metrics_data.to_csv(sep=",", header=True, index=False))
                print(
                    "---------------------------------------------------------------------------------------------------------------------------------------"
                )
        else:
            if self._all_samples_file.file_exists is False:
                self.genome.pipeline_inputs.cl_inputs.logger.warning(
                    f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: missing a required file | '{self._all_samples_file.path}'"
                )

    def calculate_range(self, series: pd.Series) -> "pd.Series":
        """
        Calculate the range (max - min) of the Timedelta values.

        Args:
            series (pd.Series): an existing DataFrame column.

        Returns:
            series (pd.Series): a new DataFrame columns
        """
        return series.max() - series.min()

    def get_timedelta_str(self, tdelta: "timedelta") -> str:
        """
        Re-formats a '0 days 00:00:00' timedelta object back into a '0-00:00:00' from SLURM. Used internally by summarize_resources() only.
        """
        d = {"days": tdelta.days}
        d["hours"], remainder = divmod(tdelta.seconds, 3600)
        d["minutes"], d["seconds"] = divmod(remainder, 60)
        for key, value in d.items():
            if key != "days":
                d[key] = str(value).zfill(2)
        return f'{d["days"]}-{d["hours"]}:{d["minutes"]}:{d["seconds"]}'

    def str_mem(self, mem: float) -> str:
        """
        Re-formats a float memory used back to '00.00G' string.
        Used internally by process_resources() and summary().
        """
        mem_string = f"{round(mem,2)}G"
        return mem_string

    def summarize_resources(self) -> None:
        """
        Calculate resource usage stats across multiple samples.
        """        
        # Summarize elapsed wall time (NOT CORE TIME)
        duration_summary = pd.DataFrame(
            self._metrics_data.groupby("JobType").agg(
                job_count=("Elapsed_Time", "count"),
                min=("Elapsed_Time", "min"),
                mean=("Elapsed_Time", "mean"),
                median=("Elapsed_Time", "median"),
                max=("Elapsed_Time", "max"),
                range=("Elapsed_Time", self.calculate_range),
            )
        )
        # Convert the timedelta objects back to a DD-HH:MM:SS format string
        duration_summary[["min", "mean", "median", "max", "range"]] = duration_summary[
            ["min", "mean", "median", "max", "range"]
        ].map(self.get_timedelta_str)

        # Ungroup
        duration_summary.reset_index(inplace=True)

        if self.genome.pipeline_inputs.cl_inputs.debug_mode:

            self.genome.pipeline_inputs.cl_inputs.logger.debug(
                f"Duration\n---------------------------------------------\n{duration_summary.to_csv(sep=",", header=True, index=False)}\n---------------------------------------------",
            )

        # Summarize REAL memory usage
        real_mem_summary = pd.DataFrame(
            self._metrics_data.groupby("JobType").agg(
                min=("MaxRSS_G", "min"),
                mean=("MaxRSS_G", "mean"),
                median=("MaxRSS_G", "median"),
                max=("MaxRSS_G", "max"),
                range=("MaxRSS_G", self.calculate_range),
            )
        )

        # Convert the memory floats back to a formatted string
        real_mem_summary[["min", "mean", "median", "max", "range"]] = real_mem_summary[
            ["min", "mean", "median", "max", "range"]
        ].map(self.str_mem)

        # Ungroup
        real_mem_summary.reset_index(inplace=True)

        if self.genome.pipeline_inputs.cl_inputs.debug_mode:
            self.genome.pipeline_inputs.cl_inputs.logger.debug(
                f"Memory Used\n---------------------------------------------\n{real_mem_summary.to_csv(sep=",", header=True, index=False)}\n---------------------------------------------"
            )

        # Merge and clean up the two dfs ------------
        # remove duplicate columns to avoid 2 JobType columns with the same value
        job_types = list(duration_summary["JobType"])
        duration_summary.drop("JobType", inplace=True, axis=1)
        real_mem_summary.drop("JobType", inplace=True, axis=1)

        # Join the 2 dataframes
        self._summary_data = duration_summary.join(
            real_mem_summary, lsuffix="_runtime", rsuffix="_mem"
        )

        # Add back the job type column
        self._summary_data.insert(0, "job_type", job_types)

        # TO DO: maybe include CORE HOURS instead of just wall hours?

        self.genome.pipeline_inputs.cl_inputs.logger.info(
            f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: resources used summary\n===============================\n{self._summary_data.to_csv(sep=",", header=True, index=False)}\n===============================",
        )
        self.genome.pipeline_inputs.cl_inputs.logger.info(
            f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: finished all {int(self._n_jobs):,} jobs\n======= {int(self._n_jobs - len(self._skipped_jobs)):,}-of-{int(self._n_jobs):,} JOBS =======\nTOTAL CORE HOURS = {int(self._total_hours):,} | {self._core_hours_str}\n===============================",
        )

        if len(self._skipped_jobs) > 0:
            self.genome.pipeline_inputs.cl_inputs.logger.warning(
                f"{len(self._skipped_jobs)} SLURM jobs were not included in the CPUTime total. Their job numbers are:\n{self._skipped_jobs}",
            )

    def write_summary(self) -> None:
        """
        If dry_run mode, display the final output to the screen.

        Otherwise, write the final output to a new file.
        """
        # Write the contents of the multi-sample summary CSV file
        if self._summary_file.file_exists is False:
            self._summary_file.write_dataframe(
                df=self._summary_data,
                keep_header=True,
                keep_index=False,
                delim=",",
                )
            self._summary_file.check_status(should_file_exist=True)
        elif self.genome.pipeline_inputs.cl_inputs.overwrite:
            self.genome.pipeline_inputs.cl_inputs.logger.info(
                f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: --overwrite=True, re-writing the multi-sample summary file | '{self._summary_file.path}'"
            )
            self._summary_file.write_dataframe(df=self._metrics_data,
                                        keep_header=True,
                                        keep_index=False,
                                        delim=",")
            self._summary_file.check_status(should_file_exist=True)
        else:
            self.genome.pipeline_inputs.cl_inputs.logger.info(
                f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: --overwrite=False, found the multi-sample summary file | '{self._summary_file.path}'"
            )

    def generate_intermediates(self) -> None:
        """
        Combine the single-sample functions of the benchmark class into a single, callable function.
        """
        self.find_job_logs()
        self.process_resources(chunk_size=1)
        self.write_intermediates()

    def generate_summary(self) -> None:
        """
        Combine the multi-sample functions of the benchmark class into a single, callable function.
        """
        self.load_all_samples()
        self.summarize_resources()
