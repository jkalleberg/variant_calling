#!/usr/bin/bash
## scripts/setup/modules.sh

echo "=== scripts/setup/modules.sh start > $(date)"

echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Wipe Modules... "
module purge
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Done Wipe Modules"

echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Loading Modules... "

# Enable "conda activate" rather than,
# using "source activate"
module load miniconda3/4.10.3_gcc_12.3.0
export CONDA_BASE=$(conda info --base)

# System Requirement to use 'conda activate' 
source ${CONDA_BASE}/etc/profile.d/conda.sh
conda deactivate

# Required for pytorch
module load cuda/11.8.0_gcc_9.5.0

# Singularity command is pre-installed, and does not require a module on Hellbender
# module load singularity/singularity

# Required for creating the Reference Genome .Dict file which is used to define chromosome names in a species-agnostistic manner
module load picard/2.26.2_gcc_12.3.0

# Modules required for post-procesing variants
module load bedtools2/2.31.1
module load bcftools/1.20_gcc_12.3.0
module load htslib/1.20_gcc_12.3.0
module load samtools/1.20_gcc_12.3.0

# module load bcftools/1.17
# module load htslib/1.17
# module load samtools/1.17

echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Done Loading Modules"

echo -e "$(date '+%Y-%m-%d %H:%M:%S') INFO: Conda Base Environment:\n${CONDA_BASE}"

# Check software versions
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: BCFtools Version -------------------"
bcftools --version | head -n3
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Python Version -------------------"
python3 --version
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Java Version -------------------"
java -version
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Apptainer Version -------------------"
apptainer --version

# Source DeepVariant version and CACHE Dir
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Adding Apptainer variables... "
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: This step is required to build DeepVariant image(s)"

if [ -z "$1" ]
then
    echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Using defaults, DeepVariant version 1.4.0"
    export BIN_VERSION_DV="1.4.0"
    export BIN_VERSION_DT="1.4.0"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Using inputs, DeepVariant version $1"
    export BIN_VERSION_DV="$1"
    export BIN_VERSION_DT="$1"
fi

export APPTAINER_CACHEDIR="${PWD}/APPTAINER_CACHE"
export APPTAINER_TMPDIR="${PWD}/APPTAINER_TMPDIR"
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Done adding Apptainer variables"

# Confirm that it worked
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: DeepVariant Version: ${BIN_VERSION_DV}"
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Apptainer Cache: ${APPTAINER_CACHEDIR}"
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Apptainer Tmp: ${APPTAINER_TMPDIR}"


# Activating the Bash Sub-Routine to handle errors
# NOTE: This helper function are MANDATORY for a module.sh file
#       because this how I avoid a bash command failing within a SLURM job, 
#       but the 'SLURM_JOB_STATUS' appears to have worked, when it didn't!
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Loading bash helper functions... "
source scripts/setup/helper_functions.sh
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Done Loading bash helper functions"

echo "=== scripts/setup/modules.sh > end $(date)"
