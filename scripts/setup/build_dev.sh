#!/bin/bash
# scripts/setup/build_dev.sh

echo -e "=== scripts/setup/build_dev.sh > start $(date)"

##--- NOTE: ----##
##  You must have an interactive session
##  with more mem than defaults to work!
##--------------##

if [ ! -d ./miniconda_envs/dev ] ; then
     # If missing an enviornment called "dev", 
     # initalize this env with only the anaconda package 
     conda create --yes --prefix ./miniconda_envs/dev
fi

# Then, activate the base environment to enable 'conda activate'
source ${CONDA_BASE}/etc/profile.d/conda.sh
conda deactivate

##--- Configure an environment-specific .condarc file ---##
## NOTE: Only performed once:
# Changes the (env) prompt to avoid printing the full path
conda config --env --set env_prompt '({name})'

# Put the package download channels in a specific order
conda config --env --add channels defaults
conda config --env --add channels bioconda
conda config --env --add channels conda-forge

# Download packages flexibly
conda config --env --set channel_priority flexible

# Install the project-specific packages in the env
conda install -p ./miniconda_envs/dev -y pip python numpy scipy pyfaidx pysam pytabix opencv

conda install -p ./miniconda_envs/dev -y bitarray cachetools intervaltree joblib matplotlib pycocotools python-dateutil pyyaml seaborn

conda install -p ./miniconda_envs/dev -y python-dotenv regex natsort mkdocs mkdocs-material black spython


###===== Notes about specific packages =====###
### Python = version required? 
### Scipy = scientific libraries for Python
### DotEnv = enables environment variable configuration across bash and python
### Regex = required for update regular expression handling
### Natsort = enables sorting of file iterators
### Mkdocs & Mkdocs-Material = used for writing Github documentation
### Black = Python formatter
### Spython = Module for executing within a Singlarity container from a Python script

echo -e "=== scripts/setup/build_dev.sh > end $(date)"
