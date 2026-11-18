#!/usr/bin/bash

#SBATCH --job-name=hadoop-pseudo
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=120G
##SBATCH --cpus-per-task=4
##SBATCH --mem=30G
#SBATCH --time=2:00:00
#SBATCH --output=hadoop.out
#SBATCH --error=hadoop.err
#SBATCH --partition=intel-128
#SBATCH --exclude=r1i0n2,r1i0n5,r1i1n0

module load singularity

set -Eeuo pipefail

# ============================================================
# User configuration
# ============================================================

BASE_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
SIF="${SIF:-$BASE_DIR/hadoop.sif}"

HDFS_PORT=9000

# YARN resources: adjust to the SLURM allocation.
YARN_MEMORY_MB=4096
YARN_VCORES=4
CONTAINER_MEMORY_MB=1024

YARN_SCRATCH_ROOT="/scratch/local/$USER"
YARN_SCRATCH="$YARN_SCRATCH_ROOT/hadoop-yarn-${SLURM_JOB_ID:-$$}"

mkdir -p "$YARN_SCRATCH/local"
mkdir -p "$YARN_SCRATCH/logs"

echo "YARN scratch: $YARN_SCRATCH"
df -h "$YARN_SCRATCH"
df -i "$YARN_SCRATCH"

# ============================================================
# Host directories
# ============================================================

CONF="$BASE_DIR/conf"
DATA="$BASE_DIR/data"
LOGS="$BASE_DIR/logs"
PIDS="$BASE_DIR/pids"
EXAMPLES="$BASE_DIR/examples"

rm -rf $DATA || true
rm -rf $LOGS || true
rm -rf $PIDS || true

mkdir -p \
    "$CONF" \
    "$DATA/namenode" \
    "$DATA/datanode" \
    "$DATA/tmp" \
    "$DATA/yarn/local" \
    "$DATA/yarn/logs" \
    "$LOGS" \
    "$PIDS" \
    "$EXAMPLES"

chmod -R u+rwX "$DATA" "$LOGS" "$PIDS"

# ============================================================
# Check environment
# ============================================================

if [[ ! -f "$SIF" ]]; then
    echo "ERROR: SIF image not found: $SIF"
    exit 1
fi

if ! command -v singularity >/dev/null 2>&1; then
    echo "ERROR: singularity command not found."
    exit 1
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "WARNING: This script is intended to run through sbatch."
fi

echo "============================================================"
echo "Hadoop pseudo-distributed deployment"
echo "SLURM job: ${SLURM_JOB_ID:-interactive}"
echo "Host: $(hostname)"
echo "SIF: $SIF"
echo "============================================================"

# ============================================================
# Singularity execution helper
# ============================================================

BIND_ARGS=(
    --bind "$CONF:/etc/hadoop-runtime"
    --bind "$DATA:/hadoop-data"
    --bind "$LOGS:/hadoop-logs"
    --bind "$PIDS:/hadoop-pids"
    --bind "$EXAMPLES:/examples"
    --bind "$YARN_SCRATCH:/yarn-local"
)

run_container() {
    singularity exec "${BIND_ARGS[@]}" "$SIF" \
        bash -c '
            export HADOOP_LOG_DIR=/hadoop-logs
            export HADOOP_PID_DIR=/hadoop-pids
            export HADOOP_TMP_DIR=/hadoop-data/tmp

            export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"

            exec "$@"
        ' bash "$@"
}

# ============================================================
# Stop daemons on exit, including failure
# ============================================================

cleanup() {
    echo "Stopping Hadoop daemons..."

    run_container mapred --daemon stop historyserver || true
    run_container yarn --daemon stop nodemanager || true
    run_container yarn --daemon stop resourcemanager || true
    run_container hdfs --daemon stop datanode || true
    run_container hdfs --daemon stop namenode || true
    rm -rf "$YARN_SCRATCH" || true

    echo "Hadoop shutdown complete."
}

trap cleanup EXIT INT TERM

# ============================================================
# Format NameNode only on first execution
# ============================================================

if [[ ! -f "$DATA/namenode/current/VERSION" ]]; then
    echo "Formatting NameNode for first execution..."

    run_container hdfs namenode -format -force
else
    echo "Existing NameNode metadata found; skipping format."
fi

# ============================================================
# Start HDFS
# ============================================================

echo "Starting NameNode..."
run_container hdfs --daemon start namenode

echo "Starting DataNode..."
run_container hdfs --daemon start datanode

echo "Waiting for HDFS..."

HDFS_READY=0
for i in $(seq 1 30); do
    if run_container hdfs dfsadmin -safemode get >/dev/null 2>&1; then
        HDFS_READY=1
        break
    fi
    sleep 2
done

if [[ "$HDFS_READY" -ne 1 ]]; then
    echo "ERROR: HDFS did not become available."
    exit 1
fi

run_container hdfs dfsadmin -safemode wait

# ============================================================
# Start YARN and JobHistoryServer
# ============================================================

echo "Starting ResourceManager..."
run_container yarn --daemon start resourcemanager

echo "Starting NodeManager..."
run_container yarn --daemon start nodemanager

echo "Starting JobHistoryServer..."
run_container mapred --daemon start historyserver

sleep 5

# ============================================================
# Check services
# ============================================================

echo "Checking Hadoop processes..."
run_container jps

echo "Checking HDFS..."
run_container hdfs dfsadmin -report

echo "Checking YARN nodes..."
run_container yarn node -list

# ============================================================
# Prepare HDFS input
# ============================================================

echo "Preparing HDFS input..."

run_container hdfs dfs -mkdir -p /input

run_container hdfs dfs -put -f \
    /examples/kmeans/twitter.csv /input/twitter.csv

# Remove previous output directory if it exists.
run_container hdfs dfs -rm -r -f /iterations || true

# ============================================================
# Submit Python Hadoop Streaming job
# ============================================================

KMEANS="$PWD/examples/kmeans"
ITERATIONS="$KMEANS/iterations"

echo "Generating initial centroids..."
K=50

run_container mkdir -p $ITERATIONS/0
run_container hdfs dfs -mkdir -p /iterations/0

python3 $KMEANS/initial_centroids.py $K > $ITERATIONS/0/part-00000
run_container hdfs dfs -put -f $ITERATIONS/0/part-00000 /iterations/0/part-00000

# run_container hdfs fsck / -files -blocks

STREAMING_JAR="/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar"

for i in $(seq 1 20);
do
    run_container hadoop jar "$STREAMING_JAR" \
        -D mapreduce.job.reduces=1 \
        -D mapreduce.map.memory.mb="$CONTAINER_MEMORY_MB" \
        -D mapreduce.reduce.memory.mb="$CONTAINER_MEMORY_MB" \
        -files $KMEANS/mapper.py,$KMEANS/reducer.py \
        -mapper "python3 mapper.py $(expr $i - 1)" \
        -reducer "python3 reducer.py" \
        -input /input/twitter.csv \
        -output /iterations/$i

    run_container rm -r $ITERATIONS/$i || true
    run_container hdfs dfs -get /iterations/$i $ITERATIONS/$i

    if [ $(diff $ITERATIONS/$i/part-00000 $ITERATIONS/$(expr $i - 1)/part-00000 | wc -l) -eq 0 ]; then
        break
    fi
done


# ============================================================
# Display results
# ============================================================

echo
echo "============================================================"
echo "HADOOP STREAMING RESULTS"
echo "============================================================"

run_container hdfs dfs -cat /iterations/$i/part-*

echo
echo "============================================================"
echo "Hadoop pseudo-distributed test completed successfully."
echo "============================================================"

echo "Web interfaces (from the allocated node):"
echo "NameNode:        http://localhost:9870"
echo "ResourceManager: http://localhost:8088"
echo "JobHistory:      http://localhost:19888"

# The EXIT trap stops all daemons after the job.
