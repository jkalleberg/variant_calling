#!/bin/bash
# scripts/cluster_status.sh

echo "=== start of scripts/cluster_status.sh" $(date)
LOG_MSG="[INFO]:"

echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG Current SLURM queue:"
squeue -u $USER

echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG Currently idle resources:"
sinfo -t idle | grep idle

echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG Fairshare Status:"
sshare -a -l -A schnabelr-lab,schnabelr-umag,bac,general -u $USER

echo "=== end of scripts/cluster_status.sh" $(date)
