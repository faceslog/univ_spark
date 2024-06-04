#!/bin/bash

# Define usage function
usage() {
    echo "Usage: $0 -b <bucket-name> -c <cluster-name> -p <project-name> [-r <region>] [-z <zone>] [--master-type <master-machine-type>] [--worker-type <worker-machine-type>] [--disk-size <disk-size>] [--image-version <image-version>] [--num-workers <number-of-workers>]"
    exit 1
}

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -b|--bucket) BUCKET_NAME="$2"; shift ;;
        -c|--cluster) CLUSTER_NAME="$2"; shift ;;
        -p|--project) PROJECT_NAME="$2"; shift ;;
        -r|--region) REGION="$2"; shift ;;
        -z|--zone) ZONE="$2"; shift ;;
        --master-type) MASTER_MACHINE_TYPE="$2"; shift ;;
        --worker-type) WORKER_MACHINE_TYPE="$2"; shift ;;
        --disk-size) DISK_SIZE="$2"; shift ;;
        --image-version) IMAGE_VERSION="$2"; shift ;;
        --num-workers) NUM_WORKERS="$2"; shift ;;
        *) usage ;;
    esac
    shift
done

# Validate required arguments
[[ -z "$BUCKET_NAME" || -z "$CLUSTER_NAME" || -z "$PROJECT_NAME" ]] && usage

# Set default values if not provided
REGION=${REGION:-"europe-north1"}
ZONE=${ZONE:-"europe-north1-c"}
MASTER_MACHINE_TYPE=${MASTER_MACHINE_TYPE:-"n1-standard-4"}
WORKER_MACHINE_TYPE=${WORKER_MACHINE_TYPE:-"n1-standard-4"}
DISK_SIZE=${DISK_SIZE:-500}
IMAGE_VERSION=${IMAGE_VERSION:-"2.0-debian10"}
NUM_WORKERS=2 # Default number of workers

# Execute commands

## Create bucket
gsutil mb -p "$PROJECT_NAME" -l "$REGION" "gs://$BUCKET_NAME/"

## Create the cluster
gcloud dataproc clusters create "$CLUSTER_NAME" \
    --enable-component-gateway \
    --region "$REGION" \
    --zone "$ZONE" \
    --master-machine-type "$MASTER_MACHINE_TYPE" \
    --master-boot-disk-size "$DISK_SIZE" \
    --num-workers "$NUM_WORKERS" \
    --worker-machine-type "$WORKER_MACHINE_TYPE" \
    --worker-boot-disk-size "$DISK_SIZE" \
    --image-version "$IMAGE_VERSION" \
    --project "$PROJECT_NAME"
