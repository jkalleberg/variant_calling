#!/usr/bin/bash
# scripts/get_stats.sh
echo "=== start > ./scripts/get_stats.sh" $(date) $1

INPUT=$1
# OVERWRITE=true
OVERWRITE=false

# echo "OVERWRITE:" $OVERWRITE

if [[ -d $INPUT ]]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO]: processing an input directory | '${INPUT}'"
    IS_DIR=true
elif [[ -f $INPUT ]]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO]: processing an input file | '${INPUT}'"
    IS_DIR=false
else
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') [ERROR]: input path provided must exist | '${INPUT}'\nExiting..."
    exit 1
fi

## Verify paths:
echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO]: using inputs from | '${INPUT}'"

# Custom bash functino to combine strings
function join_by { local IFS="$1"; shift; echo "$*"; }

get_stats()
{
    file=$1
    # echo "FILE:" $file

    # Get the absolute path to the input file
    file_path=$(realpath $file)
    # echo "PATH: $file_path"

    # Get the directory name only
    directory=$(dirname "$file_path")
    # echo "Directory: $directory"

    #  split the file name on the '/' character
    ARRAY_FILE=(${file//// })
    # echo "ARRAY FILE:" ${ARRAY_FILE[@]}

    # split the input name on the '.' character
    INPUT_NAME=${ARRAY_FILE[-1]}
    ARRAY_NAME=(${INPUT_NAME//./ })
    # echo "ARRAY NAME:" ${ARRAY_NAME[@]}

    # extract just the chromosome number
    chr_name="${ARRAY_NAME[-2]}"
    
    if [[ $chr_name == "vcf" || $chr_name == "bcf" ]]; then
        # echo "not processing chr files"
        PER_CHR=false
        LOG_MSG="[INFO]:"

        if [[ ${ARRAY_NAME[-1]} == "gz" ]]; then 
            # Create a prefix for files (excludes .bcf.gz)
            PREFIX_ARRAY=${ARRAY_NAME[@]:0:${#ARRAY_NAME[@]}-2}
        else
            # Create a prefix for files (excludes .bcf.gz)
            PREFIX_ARRAY=${ARRAY_NAME[@]:0:${#ARRAY_NAME[@]}-1}
        fi 
        STATS_PREFIX=$(join_by . ${PREFIX_ARRAY[@]})
        # echo "STATS PREFIX: ${STATS_PREFIX}"

        SUMMARY_ARRAY=${ARRAY_NAME[@]:0:${#ARRAY_NAME[@]}-2}
        SUMMARY_PREFIX=$(join_by . ${SUMMARY_ARRAY[@]})
        # echo "SUMMARY PREFIX: ${SUMMARY_PREFIX}"
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO]: processing a chromosome | '${chr_name}'"
        PER_CHR=true
        LOG_MSG="[INFO] - [CHR.$chr_name]:"

        # Create a prefix for files (excludes .bcf/.vcf)
        PREFIX_ARRAY=${ARRAY_NAME[@]:0:${#ARRAY_NAME[@]}-1}
        STATS_PREFIX=$(join_by . ${PREFIX_ARRAY[@]})
        # echo "STATS PREFIX: ${STATS_PREFIX}"

        SUMMARY_ARRAY=${ARRAY_NAME[@]:0:${#ARRAY_NAME[@]}-2}
        SUMMARY_PREFIX=$(join_by . ${SUMMARY_ARRAY[@]})
        # echo "SUMMARY PREFIX: ${SUMMARY_PREFIX}"
    fi

    # Process the raw counts from 'bcftools +smpl-stats' into summary metrics csv file
    # and save to a .csv file
    PER_SAMPLE_CSV_FILE="${directory}/${SUMMARY_PREFIX}.per_sample.summary.csv"
    PER_CHR_CSV_FILE="${directory}/${SUMMARY_PREFIX}.per_chr.summary.csv"

    # Create a new .stats file with 'bcftools +smpl-stats'
    STATS_FILE="${directory}/${STATS_PREFIX}.stats"
    # echo "STATS FILE: $STATS_FILE"
    # echo "SAMPLE STATS: $PER_SAMPLE_CSV_FILE"
    # echo "CHR STATS: $PER_CHR_CSV_FILE"
    # echo "PER CHR:" $PER_CHR

    if [[ -f  $STATS_FILE ]]; then 
        echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG file already exists | '${STATS_PREFIX}.stats'"
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG running 'bcftools +smpl-stats'..."
        echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG creating a new file | '${STATS_PREFIX}.stats'"
            
        # Count variants across multisample VCF
        bcftools +smpl-stats $file_path > ${STATS_FILE}
        echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG done running 'bcftools +smpl-stats'"
    fi

    # If a stats file already exists...
    if [[ -f $STATS_FILE ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG summarizing 'bcftools +smpl-stats'..."
        ####################################
        #     Summary stats per sample     #
        ####################################

        # If processing per-chromosome VCFs...
        if [[ $PER_CHR = true ]]; then

            # Create the header row on the first chrom
            if [[ $itr -eq 0 ]]; then
                if [[ -f $PER_SAMPLE_CSV_FILE ]] && [[ $OVERWRITE == false ]]; then
                    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG overwrite=FALSE; file already exists | '${SUMMARY_PREFIX}.per_sample.summary.csv'"
                
                elif [[ -f $PER_SAMPLE_CSV_FILE ]] && [[ $OVERWRITE == true ]]; then
                    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG overwrite=TRUE; file already exists,  | '${SUMMARY_PREFIX}.per_sample.summary.csv'"
                    echo -e "CHR,IID,N_SNPs,N_INDELs,SNP_INDEL,N_Hets,N_Hom_Alts,Het_Hom_Ratio,N_Ts,N_Tv,Ts_Tv,N_Singletons" > ${PER_SAMPLE_CSV_FILE}
                else
                    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG creating a new file | '${SUMMARY_PREFIX}.per_sample.summary.csv'"
                    echo -e "CHR,IID,N_SNPs,N_INDELs,SNP_INDEL,N_Hets,N_Hom_Alts,Het_Hom_Ratio,N_Ts,N_Tv,Ts_Tv,N_Singletons" > ${PER_SAMPLE_CSV_FILE}
                fi
            fi
            
            # Convert STATS format to CSV
            # Note: Handles errors that occur with 0 for denominator!
            awk -v chr="$chr_name" '/^FLT0/ {
                if ($10 > 0) {
                    SNP_INDEL = $9/ $10 
                }
                else {SNP_INDEL = "NA"}
                if ($6 > 0) {
                    HetHomAlt = $7 / $6 
                }
                else {HetHomAlt = "NA"}

                printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",chr,$2,$9,$10,SNP_INDEL,$7,$6,HetHomAlt,$13,$14,$15,$11 
            }' ${STATS_FILE} >> ${PER_SAMPLE_CSV_FILE}
        
        # If processing a single VCF...
        else
            if [[ -f $PER_SAMPLE_CSV_FILE ]] && [[ $OVERWRITE == false ]]; then
                echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG overwrite=FALSE; file already exists | '${SUMMARY_PREFIX}.per_sample.summary.csv'"
            else
                if [[ -f $PER_SAMPLE_CSV_FILE ]] && [[ $OVERWRITE == true ]]; then
                    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG overwrite=TRUE; file already exists,  | '${SUMMARY_PREFIX}.per_sample.summary.csv'"
                    echo -e "IID,N_SNPs,N_INDELs,SNP_INDEL,N_Hets,N_Hom_Alts,Het_Hom_Ratio,N_Ts,N_Tv,Ts_Tv,N_Singletons" > ${PER_SAMPLE_CSV_FILE}
                else
                    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG creating a new file | '${SUMMARY_PREFIX}.per_sample.summary.csv'"
                    echo -e "IID,N_SNPs,N_INDELs,SNP_INDEL,N_Hets,N_Hom_Alts,Het_Hom_Ratio,N_Ts,N_Tv,Ts_Tv,N_Singletons" > ${PER_SAMPLE_CSV_FILE}
                fi

                # Convert STATS format to CSV
                ## Note: Handles errors that occur with 0 for denominator!
                awk '/^FLT0/ {
                    if ($10 > 0) {
                        SNP_INDEL = $9/ $10 
                    }
                    else {SNP_INDEL = "NA"}
                    if ($6 > 0) {
                        HetHomAlt = $7 / $6 
                    }
                    else {HetHomAlt = "NA"}

                    printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",$2,$9,$10,SNP_INDEL,$7,$6,HetHomAlt,$13,$14,$15,$11 
                }' ${STATS_FILE} >> ${PER_SAMPLE_CSV_FILE}
            fi
        fi
        
        ########################################
        #     Summary stats per chromosome     #
        ########################################
        NUM_SAMPLES=$(bcftools query --list-samples ${file_path} | wc -l)

        # If processing per-chromosome VCFs...
        if [[ $PER_CHR = true ]]; then

            # Create the header row on the first chrom
            if [[ $itr -eq 0 ]]; then

                # Convert STATS format to CSV
                if [[ -f $PER_SAMPLE_CSV_FILE ]] && [[ $OVERWRITE == false ]]; then
                    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG overwrite=FALSE; file already exists | '${SUMMARY_PREFIX}.per_chr.summary.csv'"
                elif [[ -f $PER_SAMPLE_CSV_FILE ]] && [[ $OVERWRITE == true ]]; then
                    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG overwrite=TRUE; file already exists,  | '${SUMMARY_PREFIX}.per_chr.summary.csv'"
                    echo -e "N_SAMPLES,CHR,N_SNPs,N_INDELs,N_SINGLETONS,SNP_INDEL,N_Ts,N_Tv,Ts_Tv" > ${PER_CHR_CSV_FILE}
                else
                    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG creating a new file | '${SUMMARY_PREFIX}.per_chr.summary.csv'"
                    echo -e "N_SAMPLES,CHR,N_SNPs,N_INDELs,N_SINGLETONS,SNP_INDEL,N_Ts,N_Tv,Ts_Tv" > ${PER_CHR_CSV_FILE}
                fi
            fi

            ## Note: Handles errors that occur with 0 for denominator!
            awk -v samples="$NUM_SAMPLES" -v chr="$chr_name" '/^SITE0/ {
                if ($4 > 0) {
                    SNP_INDEL = $3/ $4
                }
                else {SNP_INDEL = "NA"}

                printf "%s,%s,%s,%s,%s,%s,%s,%s,%s\n",samples,chr,$3,$4,$5,SNP_INDEL,$6,$7,$8
            }' ${STATS_FILE} >> ${PER_CHR_CSV_FILE}
        else
            echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_MSG missing per-chr inputs, skipping per-chr summary | '${SUMMARY_PREFIX}.per_chr.summary.csv'" 
        fi
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') [WARNING]: missing a required input file | '${SUMMARY_PREFIX}.${chr_name}.stats'"
        echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR]: unable to summarize 'bcftools +smpl-stats'"
        echo "Exiting..."
        exit 1
    fi
}

if [ $IS_DIR = false ]; then
    get_stats $INPUT

elif [ $IS_DIR = true ]; then

    # Characterize Variant Calls Across Multiple Per-Chromosome VCFs/BCFs
    # For each chromosome VCF/BCF...
    # sort the file names
    files_sorted=($(find $INPUT -type f | grep -E "\.vcf$|\.vcf\.gz$|\.bcf$|\.bcf\.gz$" | sort -V))

    itr=0

    for filename in "${files_sorted[@]}"; do    

        get_stats $filename

        # increment by 1
        itr=$((itr+1))
    done
fi

echo "=== end > ./scripts/run/prep_data/get_stats.sh" $(date)