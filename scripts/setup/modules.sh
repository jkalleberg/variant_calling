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

# Required for creating the Reference Genome .Dict file which is used to define chromosome names in a species-agnostistic manner
module load picard/2.26.2_gcc_12.3.0

# Modules required for post-procesing variants
module load bedtools2/2.31.1
module load bcftools/1.17
module load htslib/1.17
# module load tabix/2013-12-16

echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Done Loading Modules"

echo -e "$(date '+%Y-%m-%d %H:%M:%S') INFO: Conda Base Environment:\n${CONDA_BASE}"
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Python Version:"
python3 --version

# Activating the Bash Sub-Routine to handle errors
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Loading bash helper functions... "
source scripts/setup/helper_functions.sh
echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Done Loading bash helper functions"

echo "=== scripts/setup/modules.sh > end $(date)"
