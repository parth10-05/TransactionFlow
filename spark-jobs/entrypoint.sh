#!/bin/bash
# =============================================================================
# TransactionFlow Spark Streaming Job — entrypoint
# =============================================================================
# Always submits to the Spark standalone cluster (spark://spark-master:7077).
# NO local-mode fallback exists in the Compose deployment — if the cluster URL
# is wrong the job fails immediately so the problem is visible, not silent.
#
# Spark conf summary for the 2-worker Docker cluster:
#   --executor-memory 1g    : fits inside the 2G worker memory budget
#   --executor-cores 1      : one core per executor, one executor per worker
#   --conf spark.cores.max=2: cap total cores so each worker gets exactly 1
#   shuffle.partitions=4    : low enough to avoid excessive shuffle overhead
# =============================================================================

set -e

# ── Guard: refuse to start without an explicit cluster master URL ─────────────
if [ -z "${SPARK_MASTER_URL}" ]; then
  echo "ERROR: SPARK_MASTER_URL is not set." >&2
  echo "       Set SPARK_MASTER_URL=spark://spark-master:7077 in .env and restart." >&2
  exit 1
fi

case "${SPARK_MASTER_URL}" in
  local*|LOCAL*)
    echo "ERROR: SPARK_MASTER_URL='${SPARK_MASTER_URL}' is a local-mode URL." >&2
    echo "       This deployment requires the Spark standalone cluster." >&2
    echo "       Set SPARK_MASTER_URL=spark://spark-master:7077 in .env." >&2
    exit 1
    ;;
esac

echo "================================================="
echo " TransactionFlow Spark Streaming Job"
echo " Master : ${SPARK_MASTER_URL}"
echo " Mode   : cluster (standalone)"
echo "================================================="

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --conf spark.cores.max=2 \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000 \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
  --conf "spark.sql.streaming.checkpointLocation=${HDFS_CHECKPOINT_PATH:-hdfs://hdfs-namenode:9000/data/transactionflow/checkpoints}" \
  --class com.acido303.transactionflow.spark.TransactionFlowStreamingJob \
  /app/spark-job.jar
