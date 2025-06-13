#!/bin/bash
# scripts/start_interactive.sh
# An example script of requesting interactive resources for Mizzou's Hellbender SLURM Cluster
# NOTE: You will need to change this to match your own setup, such as 
# altering the partition name  and qos (i.e. 'Interactive') or,
# altering your account (i.e. 'schnabelr-lab')

# srun --pty -p gpu --time=0-04:00:00 -A schnabelr-lab /bin/bash
srun --pty -p schnabelr-lab --time=0-05:00:00 --mem=30G -A schnabelr-lab /bin/bash
# srun --pty -p interactive --time=0-04:00:00 --mem=30G -A schnabelr-lab /bin/bash
# srun --pty -p interactive --time=0-05:00:00 --mem=30G /bin/bash
# srun --pty -p general --time=0-08:00:00 --mem=30G /bin/bash
# srun --pty -p general,schnabelr-umag --time=0-04:00:00 --mem=30G -A schnabelr-umag /bin/bash

