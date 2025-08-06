#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Usage: $0 <number_of_workers>"
  exit 1
fi

WORKER_COUNT="$1"

# Deploys N EC2 instances with S3 access and running a script at initialization
# (shall connect to the orchestrator at initialization)
INSTANCE_ROLE="MyEC2S3AccessRole"
KEY_PAIR_NAME="worker-keys" # Use your .pem key name
#AMI_ID="ami-062abdb4b1d1cc0bf" # Use the worker AMI ID old (without aws credentials provider timeout)
AMI_ID="ami-0e1ac9f630b80a8df"
INSTANCE_TYPE="t3.medium"

aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_PAIR_NAME" \
  --iam-instance-profile Name="$INSTANCE_ROLE" \
  --user-data file://init_script.sh \
  --count "$WORKER_COUNT" \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=PyEdgeCompute-worker}]' \
  --associate-public-ip-address # Or associate with a subnet that has internet access