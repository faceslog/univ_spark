#!/bin/bash

usage() {
    echo "Usage: $0 -b <bucket-name> -c <cluster-name> -p <project-name> [-r <region>] [-z <zone>] [-d <data-url>] [-i <iter-count>] --job <job-type>"
    echo "Example: $0 -b bernard-le-bucket -c bernard-le-cluster -p page-rank-401112 --job spark"
    exit 1
}

run_pig() {
    echo "Running Pig job..."
    start_time_pig=$(date +%s)
    gcloud dataproc jobs submit pig --cluster ${CLUSTER} -f gs://${BUCKET}/src/pypig/pigrank.py
    end_time_pig=$(date +%s)
    duration_pig=$((end_time_pig - start_time_pig))

    gsutil cp -r gs://${BUCKET}/out/pig/pagerank_data_${ITER}/* out/pig/all
    gsutil cp -r gs://${BUCKET}/out/pig/pagerank_max/* out/pig/max

    echo "Time for Pig: ${duration_pig} seconds" >> out/pig/time.txt
    echo "Time for Pig: ${duration_pig} seconds"
}

run_spark() {
    echo "Running Spark job..."
    start_time_spark=$(date +%s)
    gcloud dataproc jobs submit pyspark gs://${BUCKET}/src/pyspark/sparkrank.py --cluster ${CLUSTER} -- ${DATA} ${BUCKET} ${ITER}
    end_time_spark=$(date +%s)
    duration_spark=$((end_time_spark - start_time_spark))

    gsutil cp -r gs://${BUCKET}/out/spark/allRanks/* out/spark/all
    gsutil cp -r gs://${BUCKET}/out/spark/maxRank/* out/spark/max

    echo "Time for Spark: ${duration_spark} seconds" >> out/spark/time.txt
    echo "Time for Spark: ${duration_spark} seconds"
}

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -b|--bucket) BUCKET="$2"; shift ;;
        -c|--cluster) CLUSTER="$2"; shift ;;
        -p|--project) PROJECT="$2"; shift ;;
        -r|--region) REGION="$2"; shift ;;
        -z|--zone) ZONE="$2"; shift ;;
        -d|--data) DATA="$2"; shift ;;
        -i|--iterations) ITER="$2"; shift ;;
        --job) JOB_TYPE="$2"; shift ;;
        *) usage ;;
    esac
    shift
done

# Validate required arguments
if [[ -z "${BUCKET}" || -z "${CLUSTER}" || -z "${PROJECT}" || -z "${JOB_TYPE}" ]]; then
    usage
fi

if [[ "$JOB_TYPE" != "pig" && "$JOB_TYPE" != "spark" ]]; then
    echo "Invalid job type specified. Choose either 'pig' or 'spark'."
    usage
fi

# Set default values if not provided
REGION=${REGION:-"europe-north1"}
ZONE=${ZONE:-"europe-north1-c"}
DATA=${DATA:-"gs://public_lddm_data/page_links_en.nt.bz2"}
ITER=${ITER:-1}

## Config
gcloud config set project ${PROJECT}
gcloud config set dataproc/region ${REGION}

WORKERS=$(gcloud dataproc clusters describe cluster-93ea --region europe-west6 | grep -oP '^\s*numInstances:\s*\K.' | tail -c2)
read -p "Set worker count [current = ${WORKERS}]: " NEW_WORKERS

if [ ${WORKERS} != ${NEW_WORKERS} ]
then
    echo "Setting worker count..."
    gcloud dataproc clusters update ${CLUSTER} --num-workers=${NEW_WORKERS}
fi

## Clean
gcloud storage rm -r gs://${BUCKET}/src
gcloud storage rm -r gs://${BUCKET}/out
mkdir -p out/
rm -rf out/*
mkdir -p out/pig/all
mkdir -p out/pig/max
mkdir -p out/spark/all
mkdir -p out/spark/max

# Create a temporary version of the pigrank.py script with placeholders replaced
TMP_PIG_SCRIPT="tmp_pigrank.py"
sed -e "s/PLACEHOLDER_BUCKET/${BUCKET}/" \
    -e "s/PLACEHOLDER_ITER/${ITER}/" \
    -e "s|PLACEHOLDER_INPUTFILE|${DATA}|g" \
    src/pypig/pigrank.py > ${TMP_PIG_SCRIPT}

gcloud storage cp -r src gs://${BUCKET}/
gcloud storage cp -r ${TMP_PIG_SCRIPT} gs://${BUCKET}/src/pypig/pigrank.py
rm ${TMP_PIG_SCRIPT}

# Run job based on job type
case "$JOB_TYPE" in
    pig)
       run_pig
       ;;
    spark)
       run_spark
       ;;
    *) echo "Invalid job type specified."
       usage ;;
esac
