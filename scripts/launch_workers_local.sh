#!/bin/bash

# Check if a number was passed as argument
if [ -z "$1" ]; then
    echo "Usage: ./$0 <number_of_workers>"
    exit 1
fi

NUM_WORKERS=$1

# Run worker scripts in parallel
for ((i=1; i<=NUM_WORKERS; i++)); do
    node worker/src/main.mjs --orch http://localhost:3000 --storage http://localhost:9002 &
done

# Wait for all background processes to finish
wait
