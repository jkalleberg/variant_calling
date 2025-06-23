#!/bin/bash

# Custom Sub-process Status Handling Routine and returns the error code 
# effectively terminates a SLURM job with the sub-process's command status
# to prevent down-stream Bash commands from running if first command fails
## USAGE: source ./scripts/setup/helper_functions.sh

### FAILURE EXAMPLE: 
# (dev)[jakth2@c102 variant_calling]$ bash .scripts/setup/test_fail_handler.sh && capture_status test_failure || capture_status test_failure
# PRETEND JOB FAILURE
# exit 1
# A first argument was provided.
# A second argument was NOT provided.
# ERROR: test_failure
# exit 1

### SUCCESS EXAMPLE:
# (dev)[jakth2@c102 variant_calling]$ bash ./scripts/setup/test_success_handler.sh && capture_status test_success || capture_status test_success
# PRETEND JOB WORKED
# A first argument was provided.
# A second argument was NOT provided.
# SUCCESS: test_success
# exit 0

capture_status()
{
    status=$?
    # echo "Exit Status:" $status

    # Determine if sub-routine completed without throwing an error code
    if [ $status -ne 0 ]; then
        STATUS_TYPE="ERROR"
    elif [ $status -eq 0 ]; then ## SUCCESS HANDLER
        STATUS_TYPE="SUCCESS"
    fi

    # Determine if a message was provided as a command line argument
    if [ "$#" -ge 1 ]; then
        # echo "A first argument was provided."
        MESSAGE="${STATUS_TYPE}: ${1}"
    else
        # echo "A first argument was NOT provided."
        MESSAGE="${STATUS_TYPE}"
    fi 

    # Determine if an output file was provided as a command line argument.
    if [ "$#" -ge 2 ]; then
        # echo "A second argument was provided."
        echo "${MESSAGE}" >> $2
    else
        # echo "A second argument was NOT provided."
        echo "${MESSAGE}"
    fi

    # echo "exit ${status}"
    exit ${status}

}
