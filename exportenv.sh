#!/bin/bash

# Script to export variables from .env file to environment variables

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found."
    exit 1
fi

# Read each line from .env file
while IFS= read -r line || [ -n "$line" ]; do
    # Skip empty lines and comments
    if [[ -z "$line" || "$line" =~ ^# ]]; then
        continue
    fi

    # Export the variable
    export "$line"
done < .env

echo "Environment variables loaded successfully from .env file."
