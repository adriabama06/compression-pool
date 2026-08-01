#!/bin/bash

# Directory
OUTPUTS="outputs"
INPUTS="inputs"

# For each file in outputs/
for f in "$OUTPUTS"/*; do
    # Get filename
    filename=$(basename "$f")
    
    # Check if exist in inputs/
    if [ -f "$INPUTS/$filename" ]; then
        # Remove file from input
        rm "$INPUTS/$filename"
    fi
done
