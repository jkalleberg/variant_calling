# Primary package manager
module load mamba/1.4.2

# Create a new mamba environmment
if [ ! -d ./mamba_envs/vcfstats ] ; then
     # If missing a directory contatining this mamba environment called "vcfstats"
     mamba create -p ./mamba_envs/vcfstats 
else
     echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: mamba environment exists | './mamba_envs/vcfstats'"
fi

# System Requirement to use 'conda activate' 
source ${CONDA_BASE}/etc/profile.d/conda.sh
conda deactivate

# Then, activate the new environment
conda activate ./mamba_envs/vcfstats

# mamba install -y bcftools vcftools python matplotlib numpy tectonic

# (vcfstats)[jakth2@c102 variant_calling]$ which bcftools
# /mnt/pixstor/schnabelr-drii/WORKING/jakth2/variant_calling/mamba_envs/vcfstats/bin/bcftools
# (vcfstats)[jakth2@c102 variant_calling]$ bcftools -v
# bcftools 1.22
# Using htslib 1.22
# Copyright (C) 2025 Genome Research Ltd.
# License GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>
# This is free software: you are free to change and redistribute it.
# There is NO WARRANTY, to the extent permitted by law.
# (vcfstats)[jakth2@c102 variant_calling]$ bcftools stats /mnt/pixstor/schnabelr-drii/WORKING/jakth2/CATTLE_TEST/1051/1051.vcf.gz > file.vchk
# (vcfstats)[jakth2@c102 outdir]$ plot-vcfstats -p outdir file.vchk 
#### PLOT-VCFSTATS WILL STILL "FAIL", but 
# (vcfstats)[jakth2@c102 variant_calling]$ plot-vcfstats -p outdir file.vchk 
#### RUNNING THIS COMMAND AFTER WILL GET IT TO WORK
# (vcfstats)[jakth2@c102 variant_calling]$ tectonic outdir/summary.tex