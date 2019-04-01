#!/bin/bash

# Compares two image files and writes their difference to another image file.
# Exit code is 0 if the images are identical, 1 if they are different and 2 if there is an error.

if [ $# -ne 3 ]; then
    >&2 echo "usage: compare.sh <new image> <reference image> <diff image>"; exit 2
fi

NEW_FILE=$1
REF_FILE=$2
DIFF_FILE=$3

# Note: compare's exit code is unreliable across versions, rely on stderr instead.

DIFFERENCE=$(compare -metric AE $NEW_FILE $REF_FILE -compose Src -highlight-color White -lowlight-color Black $DIFF_FILE 2>&1)

if ! [[ $DIFFERENCE =~ ^[0-9]+$ ]]; then
    >&2 echo $DIFFERENCE; exit 2
elif [ $DIFFERENCE -gt 0 ]; then
    exit 1
fi
