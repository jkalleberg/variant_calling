#!/bin/bash
# scripts/setup/install_cue.sh

echo -e "=== scripts/setup/install_cue > start $(date)"

git clone git@github.com:jkalleberg/cue.git ../cue

wget --directory-prefix=tutorial/existing_ckpts/Cue https://storage.googleapis.com/cue-models/latest/cue.v2.pt

echo -e "=== scripts/setup/install_cue > end $(date)"