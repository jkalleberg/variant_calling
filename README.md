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

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <h3 align="center">Generic Variant Caller Pipeline</h3>

  <p align="center">
    An awesome README template to jumpstart your projects!
    <br />
    <a href="https://github.com/jkalleberg/variant_calling/tree/working-jk/docs"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/jkalleberg/variant_calling">View Demo</a>
    &middot;
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
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>


<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple example steps.

### Prerequisites

Things you need to use the software. The pipeline expects these prereqs to be contained within the `scripts/setup/modules.sh` script.
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

<p align="right">(<a href="#install-top">back to install top</a>)</p>

2. Create a local copy of the source code:

    _Editing files within this cloned repository can create a tangled knot of Git commits if you don't know what you're doing! If you anticipate making edits to the code, you will need to **FORK** the repository (create your own version)._
   
    ```sh
    git clone git@github.com:jkalleberg/variant_calling.git
    ```
<p align="right">(<a href="#install-top">back to install top</a>)</p>

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

<p align="right">(<a href="#install-top">back to install top</a>)</p>

4. Change git branch to avoid accidental pushes to base project:
   
    * View the existing local git branch names:
      ```sh
      git branch
      ```
      
      _Expected Output:_
      ```sh
      * main
      ```

    * Create a new git branch:
      NOTE: `-b <branch_name>` will need to be a unique (not one listed on GitHub!)
    
      ```sh
      git checkout -b working-jk
      ```
    
      _Expected Output:_
      ```sh
      Switched to a new branch 'working-jk'
      ```

    * Confirm you are _not_ on the `main` branch:
      ```sh
      git branch
      ```

      _Expected Output:_
      ```sh
        main
      * working-jk
      ```

<p align="right">(<a href="#install-top">back to install top</a>)</p>

5. Switch to a compute node, instead of the login node:
   
   [Click here](scripts/start_interactive.sh) to view the script contents.

   ```sh
   . scripts/start_interactive.sh
   ```
   
   _Expected Output:_
   ```sh
   srun: job <########> queued and waiting for resources
   srun: job <########> has been allocated resources
   ```

   * **NOTE:** If you want to check which partition(s) or compute resources are available do the following before editing `scripts/start_interactive.sh`
  
     [Click here](scripts/cluster_status.sh) to view the script contents.
     
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
     logical_cpu       up 2-00:00:00      4   idle c[077-080]
     logical_cpu2      up 2-00:00:00      1   idle c077
     schnabelr-umag    up 28-00:00:0      1   idle c092
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

<p align="right">(<a href="#install-top">back to install top</a>)</p>

6. Activate Hellbender-specific software modules:

   After moving off the login node: `(i.e., <user>@hellbender-login variant_calling]$ -> <user>@c010 variant_calling]$)`

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

<p align="right">(<a href="#install-top">back to install top</a>)</p>

7. Create the development conda environment `(dev)`, and confirm it works:

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

<p align="right">(<a href="#install-top">back to install top</a>)</p>

8. Create the Singularity/Apptainer container, and confirm it works:

   ```sh
   . scripts/setup/build_containers.sh DeepVariant-CPU
   ```
   
   _Expected Output:_
   ```sh
   Python 3.12.11
   1.4.0
   ```

<p align="right">(<a href="#install-top">back to install top</a>)</p>


<p align="right">(<a href="#readme-top">back to top</a>)</p>
