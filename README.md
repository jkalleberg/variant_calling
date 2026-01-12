<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a id="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. See: https://github.com/othneildrew/Best-README-Template 
-->

<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->

<br />
<div align="center">
  <h3 align="center">Generic Variant Caller Pipeline</h3>

  <p align="center">
    <a href="https://github.com/jkalleberg/variant_calling/issues/new?labels=bug&template=bug-report.md">Report Bug</a>
    &middot;
    <a href="https://github.com/jkalleberg/variant_calling/issues/new?labels=enhancement&template=feature-request.md">Request Feature</a>
  </p>
</div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
        <li><a href="#setup">Setup</a></li>
      </ul>
    </li>
    <li>
      <a href="#tutorial">Tutorial</a></li>
        <ul>
          <li><a href="#inputs">Variant Calling Inputs</a></li>
          <li><a href="#quick-start">Quick Start: Cattle</a></li>
          <li><a href="#custom-inputs">Pipeline Inputs</a></li>
          <li><a href="#execution-bovine">Running the bovine-trained DV model</a></li>
          <li><a href="#execution-human">Running the human-trained DV model</a></li>
        </ul>
      </ul>
    </li>
  </ol>
</details>


<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple example steps.

### Prerequisites

The pipeline assumes the following dependencies are controlled within the `scripts/setup/modules.sh` script. If you install software manually, then you will need to update your system `$PATH` to include the executable software.

* conda v4.10.3 
* cuda v11.8.89
* bedtools v2.31.1
* bcftools v1.20
* htslib v1.20
* samtools v1.20
* python v3.9.5
* java - openjdk v11.0.17 2022-10-18
* picard
* apptainer version 1.4.2-1.el8

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<a id="install-top"></a>
### Installation

_Below is an example of how you can install the app on the Hellbender HPC cluster._

1. Change directories to the working directory where the source code will be stored:
    ```sh
    cd /mnt/pixstor/schnabelr-ccgi-drii/WORKING/jakth2
    ```

2. Create a local copy of the source code:
 
    ```sh
    git clone git@github.com:jkalleberg/variant_calling.git
    ```

    **WARNING:** Editing files within this cloned repository can create a tangled knot of Git commits if you don't know what you're doing!
    
    If you anticipate making edits to the code, you will need to **FORK** the repository (create your own version) via GitHub and clone your copy.

    ```sh
    git clone git@github.com:<your GitHub>/variant_calling_copy.git
    ```

3. Enter the source code directory, and confirm the clone was successful:
    ```sh
    cd variant_calling
    git status
    ```

    _Expected Output:_
    ```sh
    On branch main
    Your branch is up to date with 'origin/main'.

    nothing to commit, working tree clean
    ```


<a id="setup-top"></a>
### Setup
#### Creating the computing environment:

1. Switch to a compute node, instead of the login node:
   
   [Click here](scripts/start_interactive.sh) to view the script contents.

   **WARNING:** This script will require manual edits to match the configuration of your SLURM-based HPC cluster.

   ```sh
   . scripts/start_interactive.sh
   ```
   
   _Expected Output:_
   ```sh
   srun: job <########> queued and waiting for resources
   srun: job <########> has been allocated resources
   ```

   **NOTE:** If you want to check which partition(s) or compute resources are available, you can edit the included helper script:
  
   [Click here](scripts/cluster_status.sh) to view the script contents.
     
   **WARNING:** This script will require manual edits to match the configuration of your SLURM-based HPC cluster.
   
   ```sh
   . ./scripts/cluster_status.sh
   ```

   _Example Output:_
   ```sh
   === start of scripts/cluster_status.sh Fri Aug 29 12:06:32 CDT 2025
   2025-08-29 12:06:32 [INFO]: Current SLURM queue:
               JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
            10079692 interacti     bash   jakth2  R      31:24      1 c141
   2025-08-29 12:06:32 [INFO]: Currently idle resources:
   requeue*          up 2-00:00:00     18   idle c[013-015,050,072,077-082,084,092-093,098,106-108]
   general           up 2-00:00:00      5   idle c[013-015,050,084]
   2025-08-29 12:06:32 [INFO]: Fairshare Status:
   Account                    User  RawShares  NormShares    RawUsage   NormUsage  EffectvUsage  FairShare    LevelFS                    GrpTRESMins                    TRESRunMins 
   -------------------- ---------- ---------- ----------- ----------- ----------- ------------- ---------- ---------- ------------------------------ ------------------------------ 
   general                               8500    0.538554 22609049537    0.800082      0.800082              0.673123                                cpu=21750135,mem=35033896004,+ 
     general                 jakth2          1    0.000362      271023    0.000010      0.000012   0.065429  30.235898                                cpu=213,mem=6555136,energy=0,+ 
   schnabelr-lab                          185    0.011721    94532621    0.003345      0.003345              3.503870                                cpu=0,mem=0,energy=0,node=0,b+ 
     schnabelr-lab           jakth2     parent    0.011721      766040    0.000027      0.008103   0.900598                                           cpu=0,mem=0,energy=0,node=0,b+ 
   schnabelr-umag                         100    0.006336   138492108    0.004901      0.004901              1.292805                                cpu=0,mem=0,energy=0,node=0,b+ 
     schnabelr-umag          jakth2     parent    0.006336         100    0.000000      0.000001   0.888330                                           cpu=0,mem=0,energy=0,node=0,b+ 
   === end of scripts/cluster_status.sh Fri Aug 29 12:06:32 CDT 2025
   ```

2. Activate HPC-cluster-specific software modules:

   [Click here](scripts/setup/modules.sh) to view the script contents.

   **WARNING:** This script will require manual edits to match the configuration of your SLURM-based HPC cluster. If you install software manually, then you will need to update your system `$PATH` to include the executable software.

   ```sh
   . scripts/setup/modules.sh 
   ```
   
   _Expected Output:_
   ```sh
   === scripts/setup/modules.sh start > Fri Aug 29 12:12:12 CDT 2025
   2025-08-29 12:12:12 INFO: Wipe Modules... 
   2025-08-29 12:12:12 INFO: Done Wipe Modules
   2025-08-29 12:12:12 INFO: Loading Modules... 
   2025-08-29 12:14:01 INFO: Done Loading Modules
   2025-08-29 12:14:01 INFO: Conda Base Environment:
   /cluster/software/SPACK/SPACK_v0.20_dev_a2/spack/opt/spack/linux-almalinux8-x86_64/gcc-12.3.0/miniconda3-4.10.3-c6moxpqnii2vbelazwz5onnnnsh3cbzm
   2025-08-29 12:14:01 INFO: BCFtools Version -------------------
   bcftools 1.20
   Using htslib 1.20
   Copyright (C) 2024 Genome Research Ltd.
   2025-08-29 12:14:01 INFO: Python Version -------------------
   Python 3.9.5
   2025-08-29 12:14:01 INFO: Java Version -------------------
   openjdk version "11.0.17" 2022-10-18
   OpenJDK Runtime Environment Temurin-11.0.17+8 (build 11.0.17+8)
   OpenJDK 64-Bit Server VM Temurin-11.0.17+8 (build 11.0.17+8, mixed mode)
   2025-08-29 12:14:02 INFO: Apptainer Version -------------------
   apptainer version 1.4.2-1.el8
   2025-08-29 12:14:02 INFO: Adding Apptainer variables... 
   2025-08-29 12:14:02 INFO: This step is required to build DeepVariant image(s)
   2025-08-29 12:14:02 INFO: Using defaults, DeepVariant version 1.4.0
   2025-08-29 12:14:02 INFO: Done adding Apptainer variables
   2025-08-29 12:14:02 INFO: DeepVariant Version: 1.4.0
   2025-08-29 12:14:02 INFO: Apptainer Cache: /mnt/pixstor/schnabelr-ccgi-drii/WORKING/jakth2/variant_calling/APPTAINER_CACHE
   2025-08-29 12:14:02 INFO: Apptainer Tmp: /mnt/pixstor/schnabelr-ccgi-drii/WORKING/jakth2/variant_calling/APPTAINER_TMPDIR
   2025-08-29 12:14:02 INFO: Loading bash helper functions... 
   2025-08-29 12:14:02 INFO: Done Loading bash helper functions
   === scripts/setup/modules.sh > end Fri Aug 29 12:14:02 CDT 2025
   ```

3. Create the development conda environment `(dev)`, and confirm it works:

   [Click here](scripts/setup/build_dev.sh) to view the script contents.

   ```sh
   . scripts/setup/build_dev.sh
   . scripts/start_dev.sh
   python3 --version        # Confirm the version of python within the conda environment
   echo $BIN_VERSION_DV     # Confirm the version of DeepVariant being used
   ```
   
   _Expected Output:_
   ```sh
   Python 3.12.11
   1.4.0
   ```

4. Create the Singularity/Apptainer container, and confirm it works:

   [Click here](scripts/setup/build_containers.sh) to view the script contents.

   **NOTE:** This script can be used to download alternative versions of DV, and the GPU-specific cluster. 

   ```sh
   . scripts/setup/build_containers.sh DeepVariant-CPU
   ```
   
   _Expected Output:_
   ```sh
   Python 3.12.11
   1.4.0
   ```


5. Download a local copy of the model checkpoint files:

   [Click here](scripts/setup/download_models.sh) to view the script contents.

   ```sh
   . scripts/setup/download_models.sh
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<a id="tutorial-top"></a>
### Tutorial

<a id="inputs"></a>
#### Variant Calling Inputs:

1. Sequencing Data (`BAM/CRAM`):
    - **NOTE:** sample name (aka lab id) will be extracted from the BAM/CRAM file
      - For example: `384425_ABC.bam` will be converted to `384425_ABC` 
      - *Assumptions: alphanumeric, and unique for each individual/sample/genome*
    - Read data and index file are located within the same directory 
    - Currently, only support pre-processed Illumina short-read WGS aligned to a reference genome
      - Future releases may support alternative sequencing platforms
  

2. Reference Genome (`FA/FASTA`):
    - Must be compatible with the provided sequencing data
  

**DeepVariant Specific Inputs:**
  1. Model checkpoint prefix:
      - All model-specific files are expected to be stored together within a directory.
      - Multiple versions of the WGS model are supported:
        ```
        (dev)[jakth2@c096 variant_calling]$ ls tutorial/existing_ckpts/DeepVariant/
        v1.4.0_withIS_default  v1.4.0_withIS_withAF  v1.4.0_withIS_withAF_bovid
        ```
      - A sub-directory's expected naming convention uses: `<version number>_<model type>`
        - For example: `v1.4.0._withIS_default` represents the default, human-genome-trained checkpoint for short-read WGS.
          - In this version, a channel called "insert size" (IS) was included by default.
        - Alternatively: `v1.4.0._withIS_withAF_bovid` represents the custom bovine-trained checkpoint created with TrioTrain.
          - This version is an extension of `v1.4.0_withIS_default`, which includes an additional "allele frequency" (AF) channel.

  2. Population allele frequencies (AF) for your species (`VCF`)
      - Required input for any "AF" version of DeepVariant
      - Must be compatible with the provided Reference Genome
      - No genotypes should be present within the VCF


**Tutorial Data Availability:**

We do not provide an example BAM/CRAM file. This quick start assumes your BAM/CRAM files are compatible with the cattle reference genome (`ARS-UCD1.2_Btau5.0.1Y`). Obtain your own local copies of these large input files using the following:
     
[Click here](scripts/setup/tutorial_cattle.sh) to view the script contents.

```sh
. scripts/setup/tutorial_cattle.sh
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<a id="quick-start"></a>
#### Quick Start:

  1. Confirm source code behaves as expected, by getting the help menu:

      ```sh
      # Get off the login node
      [jakth2@hellbender-login variant_calling]$ . scripts/start_interactive.sh

      # Activate cluster-specific modules
      [jakth2@c096 variant_calling]$ . scripts/setup/modules.sh

      # Activate the conda environment
      [jakth2@c096 variant_calling]$ . scripts/start_dev.sh

      # Get the pipeline options
      (dev)[jakth2@c096 variant_calling]$ python3 run.py -h
      ```

      _Expected Output:_
      
      ```sh
      ===== start of /cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/run.py @ 2026-01-12  12:28:32 =====
      usage: run.py [-h] [-O </path/>] [-I </path/to/dir/> or </path/to/file>] [--overwrite] [-d] [--dry-run] [-v] [--keep-jobids] [--model-config </path/file>] [-m </path/file>] [-r </path/file>] [-R </path/file_prefix_only>] [--submit-size <int>] [--submit-start <int>] [--submit-stop <int>] [--unmapped-reads <str>]

      options:
        -h, --help            show this help message and exit
        -O </path/>, --output-path </path/>
                              [REQUIRED]
                              output path
                              Location to save resulting file(s).
        -I </path/to/dir/> or </path/to/file>, --input-path </path/to/dir/> or </path/to/file>
                              [REQUIRED]
                              input path
                              If a directory is provided, multiple inputs will be identified.
        --overwrite           If True, enables re-writing files.
        -d, --debug           If True, enables printing detailed messages.
        --dry-run             If True, display results to the screen, rather than to a file.
        -v, --version         show program's version number and exit
        --keep-jobids         if True, save SLURM job numbers for calculating resource usage
        --model-config </path/file>
                              [REQUIRED]
                              input file(s) (.json)
                              defines internal parameters for a specific variant caller
                              to use multiple variant callers, provide a comma-separated list config files
                              (default: ./tutorial/data/cattle/default_config.json)
        -m </path/file>, --modules </path/file>
                              [REQUIRED]
                              input file (.sh)
                              helper script which loads the local software packages
                              (default: ./scripts/setup/modules.sh)
        -r </path/file>, --resources </path/file>
                              [REQUIRED]
                              input file (.json)
                              defines HPC cluster resources for SLURM
        -R </path/file_prefix_only>, --reference-prefix </path/file_prefix_only>
                              [REQUIRED]
                              input file prefix
                              defines naming convention for the reference genome to find similar file(s) located in the same directory
                              minimum file expectations:   
                                  (.fasta + .fai index)
                              additional expectations (created automatically if missing):
                                  reference dictionary file created with PICARD (.dict)
                                  default regions file (.bed)
        --submit-size <int>   controls the number of samples' submitted for variant calling
                              effectively rate-limits the amount of SLURM jobs submitted
                              (default: 1)
        --submit-start <int>  1-based index representing which row to start with from --input-path
                              (default: 1)
        --submit-stop <int>   1-based index representing which row to end with from --input-path
                              if None, then run all samples.
                              (default: None)
        --unmapped-reads <str>
                              [REQUIRED]
                              prefix for unmapped reads in reference genome which are excluded during variant calling
                              defaults to @SQ tag from ARS-UCD1.2_Btau5.0.1Y
                              (default: NKLS)
      ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<a id="custom-inputs"></a>
### Customize Inputs:

1. Input File (`CSV/TSV`):
    - [Click here](tutorial/data/cattle/250627_Sutovsky_samples.csv) to view an example file. 
    - One line for each sample
    - Provides the absolute path to each BAM/CRAM and index files
    - All files must exist already and be compatible with the reference genome
    
  
2. Output Path (`/path/to/directory`):
    - Ensures multiple checkpoints can be used to generate VCFs for the same samples 
    - The entered directory path is appended with `<model_type>/<checkpoint_label>`
        - For example, `/path/to/directory/deepvariant/v1.4.0_withIS_noAF`
    - This is derived from the checkpoint naming conventions described previously

3. SBATCH config file (`JSON`):
    - [Click here](tutorial/data/resources.json) to view an example file.
    - **NOTE:** variable names (aka keys with this dictionary) **must be valid SLURM SBATCH flags** 
      - The exception is `"email"` which is automatically converted into two SBATCH flags internally:
        - `--mail-type=FAIL`
        - `--mail-user=<email provided>`  
     
4. Pipeline config file (`JSON`):
    - [Click here](tutorial/data/cattle/default_config.json) to view an example file.
    - **NOTE:** variable (aka keys with this dictionary) must match expectations.
    - Valid variables include:
      - `model_type`: only "deepvariant" supported currently; future releases may support "cue" 
      - `model_version`: only "v1.4.0" supported currently; future releases may support v1.5.0+
      - `checkpoint_prefix`: relative path to an existing checkpoint directory 
      - `pop_file`: relative path to an existing PopVCF, compatible with the reference genome
      - `get_help`: if true, print the internal menu options of the Singularity/Apptainer container used
      - `output_type`: valid options include ["vcf", "g.vcf"] 
      - `keep_vcf`: creating a g.vcf automatically produces a vcf within DV. If true, keeps both.
      - `extra_args`: controls a subset of DV sub-process flags.
    - [Click here](docs/container_man_pgs/) to view a static version of all DV sub-process flags.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<a id="execution-bovine"></a>
### Usage: Bovine-trained DV-AF

  Example usage for the `v1.4.0._withIS_withAF_bovid` DeepVariant checkpoint.

  [Click here](tutorial/cattle/default_config.json) to view the default `--model-config` file.

  ```sh
  # Get off the login node
  [jakth2@hellbender-login variant_calling]$ . scripts/start_interactive.sh

  # Activate cluster-specific modules
  [jakth2@c096 variant_calling]$ . scripts/setup/modules.sh

  # Activate the conda environment
  [jakth2@c096 variant_calling]$ . scripts/start_dev.sh

  # Submit all samples (one at a time) using the variant caller pipeline
  # Number of samples depends on number of rows in -I / --input-path
  # NOTE: Here, we use --dry-run to view the contents of the SLURM job before submission
  #       Remove this flag to submit to SLURM queue immediately. 
  (dev)[jakth2@c096 variant_calling]$ python3 run.py -O ../CATTLE_TUTORIAL/ -I ./tutorial/data/cattle/250627_Sutovsky_samples.csv --reference-prefix ./tutorial/data/cattle/reference/ARS-UCD1.2_Btau5.0.1Y --dry-run
  ```

  _Expected Output:_
      
  ```sh
  ===== start of /cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/run.py @ 2026-01-12  13:23:14 =====
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN]: output will display to screen and not write to a file
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [setup]: pretending to write TSV file | '/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutor
  ial/data/cattle/reference/ARS-UCD1.2_Btau5.0.1Y.bed'
  ---------------------------------------------
  1       0       158534110
  2       0       136231102
  3       0       121005158
  4       0       120000601
  5       0       120089316
  6       0       117806340
  7       0       110682743
  8       0       113319770
  9       0       105454467
  10      0       103308737
  11      0       106982474
  12      0       87216183
  13      0       83472345
  14      0       82403003
  15      0       85007780
  16      0       81013979
  17      0       73167244
  18      0       65820629
  19      0       63449741
  20      0       71974595
  21      0       69862954
  22      0       60773035
  23      0       52498615
  24      0       62317253
  25      0       42350435
  26      0       51992305
  27      0       45612108
  28      0       45940150
  29      0       51098607
  X       0       139009144
  Y       0       43300181
  MT      0       16340

  ---------------------------------------------
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [setup]: number of potential files to process | 2
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [setup]: checking input format for 2-of-2 rows
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples]: sample_id='384425' | raw_data='/cluster/VAST/schnabelr-lab/WORKING/schnabelr/20250530_LH00642_0074_A22YFCCLT3_SutovskyP_03_SOL/384425/384425.bam'
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples]: pretending to create a new directory | '/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid'
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples]: pretending to create a new directory | '/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425'
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples]: pretending to create a new directory | '/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/jobs'
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples]: pretending to create a new directory | '/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/logs'
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples]: pretending to create a new directory | '/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/tmp'
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples] - [run_deepvariant]: missing the default G.VCF.GZ file | '384425.g.vcf.gz'
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples] - [run_deepvariant]: pretending to write a new pickle file | '/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/384425.pkl'
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: pretending to expect the following output file | '384425.g.vcf.gz'
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: using the CPU container | 'deepvariant_1.4.0.sif'
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: using the default value for an internal variable within 'make_examples' | min_base_quality=10   
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: using the default value for an internal variable within 'make_examples' | min_mapping_quality=5 
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: using the default value for an internal variable within 'make_examples' | gvcf_gq_binsize=10
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: using the default value for an internal variable within 'make_examples' | cnn_homref_call_min_gq=20
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: using the default value for an internal variable within 'make_examples' | logging_every_n_candidates=2000
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: using the default value for an internal variable within 'make_examples' | normalize_reads=False 
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: using the default value for an internal variable within 'call_variants' | batch_size=512
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: using the default value for an internal variable within 'call_variants' | num_readers=8
  2026-01-12 01:23:14 PM - [INFO] - [DRY_RUN] - [variant_calling]: file contents for '/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/jobs/call_variants.384425.sbatch'
  -------------------------------------
  #!/bin/bash
  #SBATCH --partition=general,schnabelr-lab
  #SBATCH --nodes=1
  #SBATCH -n 64
  #SBATCH --mem=490G
  #SBATCH --time=0-12:00:00
  #SBATCH --account=schnabelr-lab
  #SBATCH --mail-user=jakth2@mail.missouri.edu
  #SBATCH --mail-type=FAIL
  #SBATCH --job-name=call_variants.384425
  #SBATCH --output=/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/logs/%x_%j.out
  echo '=== SBATCH running on: '$(hostname)
  echo '=== SBATCH running directory: '${PWD}
  . /cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/scripts/setup/modules.sh
  echo $(date '+%Y-%m-%d %H:%M:%S')' INFO: Science starts now:'
  echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: using deepvariant with model.ckpt-282383 to call variants for sample=384425"
  time apptainer run -B /usr/lib/locale/:/usr/lib/locale/,/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/:/run_dir/,/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/data/cattle/reference/:/ref_path/,/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/data/cattle/reference/:/region_path/,/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/existing_ckpts/DeepVariant/v1.4.0_withIS_withAF_bovid/:/ckpt_path/,/cluster/VAST/schnabelr-lab/WORKING/schnabelr/20250530_LH00642_0074_A22YFCCLT3_SutovskyP_03_SOL/384425/:/reads_path/,/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/:/output_path/,/cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/tmp/:/temp_path/,/cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/tutorial/data/cattle/:/pop_path/ deepvariant_1.4.0.sif /opt/deepvariant/bin/run_deepvariant --model_type=WGS --num_shards=64 --sample_name=384425 --ref=/ref_path/ARS-UCD1.2_Btau5.0.1Y.fa --regions=/region_path/ARS-UCD1.2_Btau5.0.1Y.bed --customized_model=/ckpt_path/model.ckpt-282383 --reads=/reads_path/384425.bam --output_vcf=/output_path/384425.vcf.gz --output_gvcf=/output_path/384425.g.vcf.gz --make_examples_extra_args="use_allele_frequency=true,population_vcfs=/pop_path/UMAG1.POP.FREQ.vcf.gz" --intermediate_results_dir=/temp_path/
  source ${CONDA_BASE}/etc/profile.d/conda.sh
  conda deactivate
  conda activate miniconda_envs/dev
  python3 archive.py -I /cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/384425.pkl && capture_status "variant_calling:deepvariant" || capture_status "variant_calling:deepvariant"
  ------------------------------------
  2026-01-12 01:23:15 PM - [INFO] - [DRY_RUN] - [variant_calling]: pretending to submit SLURM job 1-of-1 with command:
  sbatch /cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/jobs/call_variants.384425.sbatch
  2026-01-12 01:23:15 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples] - [run_deepvariant]: number of pretend SLURM jobs submitted | 1
  2026-01-12 01:23:15 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples] - [run_deepvariant]: SBATCH job ids for current samples | ['35917060']
  2026-01-12 01:23:15 PM - [INFO] - [DRY_RUN] - [variant_calling] - [1-of-2 samples] - [run_deepvariant]: submit_size=1; iteration=1
  Waiting for you to press (c) to continue. ----------------------------------------------------------------
  ```

### Additional Usage Examples
  ```sh
  # Submit all samples (2 at a time) using the variant caller pipeline
  # Number of samples depends on number of rows in -I / --input-path
  (dev)[jakth2@c096 variant_calling]$ python3 run.py -O ../CATTLE_TUTORIAL/ -I ./tutorial/data/cattle/250627_Sutovsky_samples.csv --reference-prefix ./tutorial/data/cattle/reference/ARS-UCD1.2_Btau5.0.1Y --submit-size 2 --dry-run
  ```

  ```sh
  # Re-submit a single sample using the variant caller pipeline
  # Number of samples depends on number of rows in -I / --input-path
  # NOTE: --submit-start & --submit-stop are 1-based indexes of --input-path CSV file
  #       if N = submit-start, then submit-stop = N+1
  #       if no value for submit-stop is provided, then all remaining rows will be submitted
  (dev)[jakth2@c096 variant_calling]$ python3 run.py -O ../CATTLE_TUTORIAL/ -I ./tutorial/data/cattle/250627_Sutovsky_samples.csv --reference-prefix ./tutorial/data/cattle/reference/ARS-UCD1.2_Btau5.0.1Y --submit-start 2 --submit-stop 3 --dry-run
  ```

  ```sh
  # Re-write the SBATCH, create new intermediate outputs for 
  # a single sample using the variant caller pipeline
  # Number of samples depends on number of rows in -I / --input-path
  # NOTE: --submit-start & --submit-stop are 1-based indexes of --input-path CSV file
  #       if N = submit-start, then submit-stop = N+1
  #       if no value for submit-stop is provided, then all remaining rows will be submitted
  (dev)[jakth2@c096 variant_calling]$ python3 run.py -O ../CATTLE_TUTORIAL/ -I ./tutorial/data/cattle/250627_Sutovsky_samples.csv --reference-prefix ./tutorial/data/cattle/reference/ARS-UCD1.2_Btau5.0.1Y --submit-start 2 --submit-stop 3 --overwrite --dry-run
  ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<a id="execution-human"></a>
### Usage: Human-trained DV

  Example usage for the `v1.4.0._withIS_default` DeepVariant checkpoint.

  [Click here](tutorial/cattle/default_config.json) to view the default `--model-config` file.
  
  ```sh
  # Get off the login node
  [jakth2@hellbender-login variant_calling]$ . scripts/start_interactive.sh

  # Activate cluster-specific modules
  [jakth2@c096 variant_calling]$ . scripts/setup/modules.sh

  # Activate the conda environment
  [jakth2@c096 variant_calling]$ . scripts/start_dev.sh

  # Submit all samples (one at a time) using the variant caller pipeline
  # Number of samples depends on number of rows in -I / --input-path
  # NOTE: Here, we use --dry-run to view the contents of the SLURM job before submission
  #       Remove this flag to submit to SLURM queue immediately.
  #       Switch checkpoints by changing the --model-config file:
  (dev)[jakth2@c096 variant_calling]$ python3 run.py -O ../CATTLE_TUTORIAL/ -I ./tutorial/data/cattle/250627_Sutovsky_samples.csv --reference-prefix ./tutorial/data/cattle/reference/ARS-UCD1.2_Btau5.0.1Y --model-config ./tutorial/data/human/default_config.json --dry-run
  ```

  _Expected Output:_

  ```sh
  # Relative to the bovine-trained model, the only difference 
  # should be the different value for --customized-model=/ckpt_path/model.ckpt 
  # (instead of /ckpt_path/model.ckpt-282383)

  /opt/deepvariant/bin/run_deepvariant \
    --model_type=WGS --num_shards=64 --sample_name=384425 \
    --ref=/ref_path/ARS-UCD1.2_Btau5.0.1Y.fa \
    --regions=/region_path/ARS-UCD1.2_Btau5.0.1Y.bed \
    --customized_model=/ckpt_path/model.ckpt \
    --reads=/reads_path/384425.bam \
    --output_vcf=/output_path/384425.vcf.gz \
    --output_gvcf=/output_path/384425.g.vcf.gz \
    --intermediate_results_dir=/temp_path/
  ```

<p align="right">(<a href="#readme-top">back to top</a>)</p> 

<a id="output"></a>
#### Output

1. Each `model_type` will have its own sub-directory under `--output-path`
    ```sh
    (dev)[jakth2@c096 variant_calling]$ ls ../CATTLE_TUTORIAL/
    deepvariant
    ```

2. Each `model_version` will have its own sub-directory under `model_type`
    ```sh
    (dev)[jakth2@c096 variant_calling]$ ls ../CATTLE_TUTORIAL/deepvariant/
    v1.4.0_withIS_withAF_bovid
    ```

3. Each sample name/id will have its own sub-directory under `model_version`
    ```sh
    (dev)[jakth2@c096 variant_calling]$ ../CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/
    384425  384426
    ```

4. If keeping both vcf and g.vcf outputs, then the per-sample content should be:
    ```sh
    (dev)[jakth2@c096 variant_calling]$ ls deepvariant/v1.4.0_withIS_withAF_bovid/384425/
    384425.g.vcf.gz  384425.g.vcf.gz.tbi  384425.pkl  384425.vcf.gz  384425.vcf.gz.tbi  384425.visual_report.html  jobs  logs  tmp
    ```

    * `jobs/`: contains the SBATCH job file submitted to SLURM
    * `logs/`: contains the SBATCH job file(s) from each submission
    * `tmp/`: contains the intermediate files (~100-300G) produced by DeepVariant
        * When `run_deepvariant/` command completes, these large temp files are removed by `archive.py`
        * If the expected outputs (at least one <sample id>.g.vcf.gz or <sample id>.vcf.gz) are missing, then `archive.py` will trigger an error. This is passed to SLURM to trigger a "job failed" email to the user.

    ```sh
    (dev)[jakth2@c096 variant_calling]$ python3 archive.py -I /cluster/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TUTORIAL/deepvariant/v1.4.0_withIS_withAF_bovid/384425/384425.pkl
    ===== start of /cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/archive.py @ 2026-01-12  14:15:34 =====
    2026-01-12 02:15:34 PM - [INFO] - [archive]: running total of files reviewed | 100-of-193 file(s)
    2026-01-12 02:15:34 PM - [INFO] - [archive]: running total of files reviewed | 193-of-193 file(s)
    2026-01-12 02:15:34 PM - [INFO] - [archive]: disk space cleared after deleting 197-of-195 items | 34.657G-of-35.189G
    ===== end of /cluster/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/archive.py @ 2026-01-12  14:15:34 =====
    ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>
