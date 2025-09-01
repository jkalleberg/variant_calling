#!/bin/bash
# scripts/setup/tutorial_cattle.sh
"""
NOTES: By default, the 'variant_calling' pipeline uses this model ckpt with 'run_deepvariant'

"""
echo -e "=== scripts/setup/tutorial_cattle.sh > start $(date)"

##======= Downloading custom, cattle-trained WGS model (with Allele Frequency channel) ===================##
#   This model allows for 8 layers in the example images 
#   Meaning, it IS compatible compatible examples built with the allele frequency channel!

#   To view in web-browser, using the following link: https://zenodo.org/records/15482485 

echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Downloading custom cattle-trained 8-channel WGS model checkpoint (with InsertSize, with AlleleFreq) - DeepVariant v${BIN_VERSION_DV}"

# Download the entire directory
wget https://zenodo.org/records/15482485/files/ModelCheckpoint.tar.gz

# Extract the directory contents
tar -xvf ModelCheckpoint.tar.gz 

# Confirm download was successful
cd ModelCheckpoint
md5sum -c model.ckpt-282383.data-00000-of-00001.md5 

# Put the downloaded files in the expected directory
mkdir -p ../tutorial/existing_ckpts/DeepVariant/v${BIN_VERSION_DV}_withIS_withAF_bovid/
mv model.ckpt-282383.* ../tutorial/existing_ckpts/DeepVariant/v${BIN_VERSION_DV}_withIS_withAF_bovid/ 

# Remove temporary files
cd ../
rm -r ModelCheckpoint*

echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Done - Downloading custom cattle-trained 8-channel WGS model checkpoint (with InsertSize, with AlleleFreq)"

# Download the required bovid PopVCF 
curl --create-dirs --continue-at - https://zenodo.org/records/15482485/files/UMAG1.POP.FREQ.vcf.gz --output "./tutorial/data/cattle/pop_vcf/UMAG1.POP.FREQ.vcf.gz"

# and Reference Genome as well
wget https://zenodo.org/records/15482485/files/ReferenceGenome.tar.gz 

# Extract the directory contents
tar -xvf ReferenceGenome.tar.gz

# Confirm download was successful
cd ReferenceGenome
md5sum -c ARS-UCD1.2_Btau5.0.1Y.fa.md5

# Put the downloaded files in the expected directory
mkdir -p ../tutorial/data/cattle/reference
mv ARS-UCD1.2_Btau5.0.1Y* ../tutorial/data/cattle/reference

# Remove temporary files
cd ../
rm -r ReferenceGenome*

echo -e "=== scripts/setup/tutorial_cattle.sh > end $(date)"