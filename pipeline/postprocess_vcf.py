#!/usr/bin/python3
"""
description: 

example usage: from pipeline.postprocess_vcf import PostProcessVCF

"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, List, Union

if TYPE_CHECKING:
    from genome import Genome

# from cmd_line import CMD
from helpers.files import TestFile, File
# from helpers.outputs import find_outputs, test_outputs


@dataclass
class PostProcessVCF:
    """
    Transform Cue Default VCF for down-stream analyses.
    """

    # required parameters
    genome: "Genome"

    # optional parameters:
    custom_vcf_tags: Union[List[str], None] = field(default=None, init=False, repr=False)

    # interal parameters
    _cmd: List[str] = field(default_factory=list, repr=False, init=False)
    _custom_header: List[str] = field(default_factory=list, repr=False, init=False)
    _missing_any_outputs: bool = field(default=True, init=False, repr=False)
    _missing_final_outputs: bool = field(default=True, init=False, repr=False)
    _missing_metrics: bool = field(default=True, init=False, repr=False)

    def __post_init__(self) -> None:
        # self._command_line = CMD(inputs=self.genome.iter.inputs)
        self._compressed_vcf = TestFile(file="", inputs=self.genome.iter.inputs)
        self._indexed_vcf = TestFile(file="", inputs=self.genome.iter.inputs)
        
        # Uncomment for Cue
        # self._renamed_vcf = TestFile(file="", inputs=self.genome.iter.inputs)
        # self._sorted_vcf = WriteFile(
        #     path_to_file="", file="", inputs=self.genome.iter.inputs
        # )
        # Uncomment for trios
        # self._mendelian_pass_vcf = TestFile(file="", inputs=self.genome.iter.inputs)
        # self._mendelian_error_vcf = TestFile(file="", inputs=self.genome.iter.inputs)
        
        # if self.custom_vcf_tags is not None:
        #     self._custom_header = self.custom_vcf_tags

        # elif self.genome.group_name is None:
        #     self._custom_header = [
        #         "CHROM",
        #         "START",
        #         "END",
        #         "QUAL",
        #         "CI_START",
        #         "CI_END",
        #         "SV_TYPE",
        #         "SV_LEN",
        #         "GT",
        #     ]
        # else:
        #     truvari_columns = [
        #         "CHROM",
        #         "START",
        #         "END",
        #         "QUAL",
        #         "SV_TYPE",
        #         "SV_LEN",
        #         "NUM_COLLAPSED",
        #         "COLLAPSED_ID",
        #     ]
        #     if self.genome.trio_order is not None:
        #         self._custom_header = truvari_columns + self.genome.trio_order

    def check_all_outputs(self, verbose: bool = False, group_name: Union[str, None] = None) -> None:
        """
        Confirm that all expected metrics and intermediate files were created.
        """
        if self.genome._paths_found:
            if not self.genome._reports_dir.is_dir():
                self.genome.create_a_dir(self.genome._reports_dir)

            if (
                len(self.genome._paths_found) == self.genome._num_chrs
                and self.genome._final_genome
            ):
                self.find_concat_vcf()
                self.edit_vcf(out_file=self._concat_vcf)

        self._total_found = 0
        
        self.find_renamed_vcf()
        self.find_sorted_vcf()
        self.check_final_outputs(verbose=verbose)

        if self._renaming_file._file._file_exists:
            self._total_found += 1
        if self._renamed_vcf._file_exists:
            self._total_found += 1
        if self._sorted_vcf._file._file_exists:
            self._total_found += 1
        if self._compressed_vcf._file_exists:
            self._total_found += 1
        if self._indexed_vcf._file_exists:
            self._total_found += 1

        if group_name is None:
            self._regex = r"\d+_svs.\w+_metrics.csv"
            self.find_summary_metrics()
            self._total_found += self._outputs_found
        else:
            self._regex = r"\d+\.\w+_svs.\w+_metrics.csv"
            self.find_summary_metrics(expected_num_files=4, verbose=True)
            self._total_found += self._outputs_found

        if "filter" in self.genome.iter.inputs.args: 
            self.find_filtered_vcf()
            if self._filtered_vcf._file_exists:
                self._total_found += 1
                self._missing_filtered_vcf = False
            else:
                self._missing_filtered_vcf = True
        else:
            self._missing_filtered_vcf = False
        
        self._missing_any_outputs = self._missing_final_outputs or self._missing_metrics or self._missing_filtered_vcf


    # def finalize_vcf(self) -> bool:
    #     """
    #     Take the Cue 'svs.vcf' default output, and make it useable for downstream analyses.
    #     """
    #     # only re-name if missing renamed VCF and either final output
    #     if self._renamed_cmd and (
    #         self._missing_any_outputs or self.genome.iter.inputs.overwrite
    #     ):
    #         self._cmd = self._renamed_cmd
    #         self.edit_vcf(out_file=self._renamed_vcf)

    #     # only sort if missing sorted VCF and compressed VCF (which replaced the sorted VCF)
    #     if self._sort_cmd and (
    #         not self._sorted_vcf._file._file_exists
    #         and not self._compressed_vcf._file_exists
    #     ):
    #         self._cmd = self._sort_cmd
    #         self.edit_vcf(out_file=self._sorted_vcf._file)

    #     # only compress if missing compressed VCF and indexed VCF
    #     if self._compress_cmd and not self._indexed_vcf._file_exists:
    #         self._cmd = self._compress_cmd
    #         self.edit_vcf(out_file=self._compressed_vcf)

    #     if self._indexed_cmd:
    #         if not self._indexed_vcf._file_exists and self._compressed_vcf._file_exists:
    #             if self.genome.iter.inputs.args.check_outputs:
    #                 if self.genome.iter.inputs.dry_run_mode:
    #                     self.genome.iter.inputs.logger.warning(
    #                         f"{self.genome._log_msg}: PRETENDING TO REMOVE BAD COMPRESSED FILE | '{self._compressed_vcf.file}'"
    #                     )
    #                 else:
    #                     self.genome.iter.inputs.logger.warning(
    #                         f"{self.genome._log_msg}: REMOVING BAD COMPRESSED FILE | '{self._compressed_vcf.file}'"
    #                     )
    #                     self._compressed_vcf.path.unlink()
    #                 return False
    #         self._cmd = self._indexed_cmd
    #         self.edit_vcf(out_file=self._indexed_vcf)
    #     return True
    
    # def find_compressed_vcf(self, use_truvari: bool = False, use_consistent: bool = True) -> None:
    #     """
    #     Identify a compressed VCF file.
    #     """
    #     if self._sorted_vcf._file._file_exists:
    #         stem = self._sorted_vcf._file.path.stem
    #         suffix = self._sorted_vcf._file.path.suffix
    #         self._output = f"{stem}{suffix}.gz"
    #         _final_output = self.genome._reports_dir / self._output
    #         _input_file = self._sorted_vcf.file_path
    #     else:
    #         if use_truvari and use_consistent:
    #             self._output = f"{self.genome.group_name}_svs.consistent.vcf"
    #             _final_output = self.genome._results_dir / self._output
    #             _input_file = f"{self.genome._sample_id}_svs.truvari.vcf"
    #         elif use_truvari and not use_consistent:
    #             self._output = f"{self.genome.group_name}_svs.mendelian_errors.vcf"
    #             _final_output = self.genome._results_dir / self._output
    #             _input_file = f"{self.genome._sample_id}_svs.truvari.vcf"
    #         else:
    #             _sorted_name = f"{self.genome._sample_id}_svs.renamed.sorted.vcf"
    #             self._output = f"{_sorted_name}.gz"
    #             _input_file = self.genome._reports_dir / _sorted_name
    #             _final_output = self.genome._reports_dir / self._output

    #     self._compressed_vcf = TestFile(
    #         file=_final_output,
    #         inputs=self.genome.iter.inputs,
    #     )
    #     self._compressed_vcf.check_missing()
    #     if use_truvari:
    #         return

    #     if self._compressed_vcf._file_exists and not self.genome.iter.inputs.overwrite:
    #         self._compress_cmd = []
    #     else:
    #         self._compress_cmd = ["bgzip", str(_input_file)]

    # def find_indexed_vcf(self) -> None:
    #     """
    #     Identify a new indexed VCF file.
    #     """

    #     if self._compressed_vcf._file_exists:
    #         stem = self._compressed_vcf.path.stem
    #         suffix = self._compressed_vcf.path.suffix
    #         output_name = self.genome._reports_dir / f"{stem}{suffix}"
    #     else:
    #         output_name = self.genome._reports_dir / self._output

    #     output = f"{output_name}.tbi"

    #     self._indexed_vcf = TestFile(
    #         file=output,
    #         inputs=self.genome.iter.inputs
    #     )
    #     self._indexed_vcf.check_missing()

    #     if self._indexed_vcf._file_exists and not self.genome.iter.inputs.overwrite:
    #         self._indexed_cmd = []
    #     elif self._indexed_vcf._file_exists and self.genome.iter.inputs.overwrite:
    #         self._indexed_cmd = ["bcftools", "index", "-f", "--tbi", str(output_name)]
    #     else:
    #         self._indexed_cmd = ["bcftools", "index", "--tbi", str(output_name)]

    # def check_final_outputs(
    #     self, expected_num_outputs: int = 2, verbose: bool = False
    # ) -> None:
    #     """
    #     Determine if final output files already exist.
    #     """
    #     outputs_found = 0
    #     self.find_compressed_vcf()
    #     self.find_indexed_vcf()

    #     if self._compressed_vcf._file_exists:
    #         outputs_found += 1

    #     if self._indexed_vcf._file_exists:
    #         outputs_found += 1

    #     self._missing_final_outputs = test_outputs(
    #         file_type="final Cue file",
    #         outputs_found=outputs_found,
    #         outputs_expected=expected_num_outputs,
    #         msg=self.genome._log_msg,
    #         logger=self.genome.iter.inputs.logger,
    #         verbose=verbose,
    #     )

    # def find_renaming_file(self) -> None:
    #     """
    #     Create a text output file to add a unique sampleID to a vcf, if it doesn't exist.
    #     """
    #     _lines = [f"{self.genome._sample_id}"]

    #     self._renaming_file = WriteFile(
    #         path_to_file=self.genome._reports_dir,
    #         file=f"{self.genome._sample_id}.rename",
    #         inputs=self.genome.iter.inputs,
    #     )
    #     self._renaming_file._file.check_missing()

    #     if not self._renaming_file._file._file_exists:
    #         self.genome.iter.inputs.logger.info(
    #             f"{self.genome._log_msg}: missing the 'bcftools reheader' input file | '{self._renaming_file.file_path.name}'"
    #         )
    #         if self.genome.iter.inputs.dry_run_mode:
    #             self.genome.iter.inputs.logger.info(
    #                 f"{self.genome._log_msg}: pretending to create a new file | '{self._renaming_file.file_path}'"
    #             )
    #             print("----------------------")
    #             for l in _lines:
    #                 print(l)
    #             print("----------------------")
    #         else:
    #             self._renaming_file.write_list(_lines)

    # def find_renamed_vcf(self) -> None:
    #     """
    #     Identify a new VCF file with unique sample ID.
    #     """
    #     self.find_renaming_file()

    #     stem = self.genome._default_vcf.path.stem
    #     suffix = self.genome._default_vcf.path.suffix
    #     output = f"{self.genome._sample_id}_{stem}.renamed{suffix}"

    #     self._renamed_vcf = TestFile(
    #         file=self.genome._reports_dir / output,
    #         inputs=self.genome.iter.inputs,
    #     )
    #     self._renamed_vcf.check_missing()

    #     ## NOTE: UNABLE TO USE -T WITH OLDER VERSION OF BCFTOOLS!
    #     if self._renamed_vcf._file_exists and not self.genome.iter.inputs.overwrite:
    #         self._renamed_cmd = []
    #     else:
    #         self._renamed_cmd = [
    #             "bcftools",
    #             "reheader",
    #             "--samples",
    #             str(self._renaming_file.file_path),
    #             "--output",
    #             str(self._renamed_vcf.file),
    #             str(self.genome._default_vcf.file),
    #             "--temp-prefix",
    #             "$TMPDIR",
    #         ]

    # def find_sorted_vcf(self, input_vcf: Union[Path, None] = None) -> None:
    #     """
    #     Identify a new VCF file sorted in genomic order.
    #     """
    #     if input_vcf is None and self._renamed_vcf._file_exists:
    #         _input = self.genome._reports_dir / self._renamed_vcf.path
    #     elif input_vcf:
    #         _input = input_vcf
    #     else:
    #         _input = (
    #             self.genome._reports_dir / f"{self.genome._sample_id}_svs.renamed.vcf"
    #         )

    #     path = Path(_input.parent)
    #     stem = _input.stem
    #     suffix = _input.suffix
    #     _output = f"{stem}.sorted{suffix}"

    #     self._sorted_vcf = WriteFile(
    #         path_to_file=path,
    #         file=_output,
    #         inputs=self.genome.iter.inputs,
    #     )
    #     self._sorted_vcf._file.check_missing()

    #     if (
    #         self._renamed_vcf._file_exists
    #         and self._sorted_vcf._file._file_exists
    #         and self._compressed_vcf._file_exists
    #         and self._indexed_vcf._file_exists
    #         and not self.genome.iter.inputs.overwrite
    #     ):
    #         self._sort_cmd = []
    #     else:
    #         self._sort_cmd = [
    #             "bcftools",
    #             "sort",
    #             str(_input),
    #             "--output-file",
    #             f"{self._sorted_vcf._file.file}",
    #             "--output-type",
    #             "z",
    #             "--temp-dir",
    #             "$TMPDIR",
    #         ]

    # def find_concat_vcf(self) -> None:
    #     """
    #     Identify if the per-chr VCFs were merged into a genome-wide VCF
    #     """
    #     self._concat_vcf = TestFile(
    #         file=self.genome._default_vcf.file,
    #         inputs=self.genome.iter.inputs,
    #     )
    #     self._concat_vcf.check_missing()

    #     if not self._concat_vcf._file_exists and self.genome._paths_found:
    #         _concat_paths = []
    #         prior_reports_dir = self.genome._reports_dir
    #         for file in self.genome._paths_found:
    #             if file:
    #                 self.genome._reports_dir = Path(file).parent
    #                 self.find_compressed_vcf()
    #                 _concat_paths.append(str(self._compressed_vcf.file))

    #         if len(_concat_paths) == self.genome._num_chrs:
    #             self._cmd = [
    #                 "bcftools",
    #                 "concat",
    #                 "--output",
    #                 str(self._concat_vcf.file),
    #             ] + _concat_paths
    #             self.genome._reports_dir = prior_reports_dir
    #         else:
    #             self.genome.iter.inputs.logger.error(f"{self.genome._log_msg}: ERROR!")
    #             exit(1)

    # def edit_vcf(
    #     self,
    #     out_file: Union[TestFile, WriteFile],
    #     stream: bool = False,
    # ) -> Union[None, List[str]]:
    #     """
    #     Change data within an existing VCF file.
    #     """
    #     if isinstance(out_file, WriteFile):
    #         output_file = out_file._file
    #     else:
    #         output_file = out_file

    #     extension = output_file.path.suffix.strip(".").upper()

    #     if any("zip" in c for c in self._cmd):
    #         type = self._cmd[0]
    #     else:
    #         type = " ".join(self._cmd[:2])

    #     if stream is False:
    #         # add a TMPDIR variable if running bcftools, and if not added previously
    #         if "bcftools" in type:
    #             add_tmpdir_cmd = ["export", f"TMPDIR={self.genome._tmp_dir}/"]
    #             if not any("TMPDIR" in line for line in self._command_line._job_cmd):
    #                 self._command_line.execute(
    #                     command_list=add_tmpdir_cmd,
    #                     type=type,
    #                     interactive_mode=stream,
    #                 )

    #         ## ADD A BASH WRAPPER TO KEEP TRACK OF WHEN OTHER LINES ARE RUNNING WITHIN THE SLURM JOB
    #         _logging_cmd = [
    #             "echo",
    #             "-e",
    #             f"$(date '+%Y-%m-%d %H:%M:%S') INFO: running '{type}'",
    #         ]
    #         self._command_line.execute(
    #             command_list=_logging_cmd,
    #             type=type,
    #         )

    #     if "sort" in type and not output_file._file_exists:
    #         prior_cmd = self._cmd
    #         self.find_compressed_vcf()
    #         if not self._sorted_vcf._file._file_exists and self._compressed_vcf._file_exists:
    #             if not self.genome.iter.inputs.debug_mode:
    #                 self.genome.iter.inputs.logger.info(
    #                     f"{self.genome._log_msg}: skipping a {extension} file | '{out_file.file}'"
    #                 )
    #             return
    #         else:
    #             self._cmd = prior_cmd

    #     if self._cmd or not output_file._file_exists:
    #         output_lines = self._command_line.execute(
    #             command_list=self._cmd,
    #             type=type,
    #             keep_output=True,
    #             interactive_mode=stream,
    #         )

    #         if "query" in type and output_lines: 
    #             if not self.genome.iter.inputs.dry_run_mode:
    #                 tsv_lines = ["\t".join(self._custom_header)] + output_lines

    #                 if (isinstance(out_file, WriteFile)
    #                     and out_file._file._file_exists is False
    #                 ):
    #                     out_file.write_list(line_list=tsv_lines)
    #             else:
    #                 return output_lines

    #         if self.genome.iter.inputs.dry_run_mode:
    #             return

    #         output_file.check_existing()
    #         if not output_file._file_exists:
    #             self.genome.iter.inputs.logger.error(
    #                 f"{self.genome._log_msg}: missing a required {extension} file | '{out_file.file}'"
    #             )
    #     else:
    #         if not self.genome.iter.inputs.debug_mode:
    #             self.genome.iter.inputs.logger.info(
    #                 f"{self.genome._log_msg}: found an existing {extension} file | '{out_file.file}'"
    #             )

   

    # def find_summary_metrics(
    #     self, expected_num_files: int = 2, verbose: bool = False
    # ) -> None:
    #     """_summary_"""
    #     if self.genome.group_name is None:
    #         _path = self.genome._reports_dir
    #     else:
    #         _path = self.genome._results_dir
    #     (
    #         metrics_exist,
    #         self._outputs_found,
    #         files,
    #     ) = find_outputs(
    #         match_pattern=self._regex,
    #         file_type="summary metrics CSV file",
    #         search_path=_path,
    #         msg=self.genome._log_msg,
    #         logger=self.genome.iter.inputs.logger,
    #         debug_mode=self.genome.iter.inputs.debug_mode,
    #         dryrun_mode=self.genome.iter.inputs.dry_run_mode,
    #     )

    #     self._missing_metrics = test_outputs(
    #         file_type="summary metrics CSV file",
    #         outputs_found=self._outputs_found,
    #         outputs_expected=expected_num_files,
    #         msg=self.genome._log_msg,
    #         logger=self.genome.iter.inputs.logger,
    #         verbose=verbose,
    #     )

    # def find_filtered_vcf(self) -> None:
    #     output = f"{self.genome._sample_id}.qual{self.genome.iter.inputs.args.filter}.vcf"
    #     self._filtered_vcf = TestFile(
    #         file=self.genome._reports_dir / output,
    #         inputs=self.genome.iter.inputs,
    #     )
    #     self._filtered_vcf.check_missing()