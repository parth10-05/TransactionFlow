package com.acido303.transactionflow.reporting.controller;

import com.acido303.transactionflow.reporting.entity.SparkJobMetrics;
import com.acido303.transactionflow.reporting.repository.SparkJobMetricsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Exposes a lightweight HDFS-status summary backed by Spark-reported write
 * metrics stored in PostgreSQL.
 *
 * <p><strong>Limitation:</strong> file counts and checkpoint status reflect what
 * the Spark job last wrote to PostgreSQL, <em>not</em> a live query against the
 * HDFS NameNode.  They confirm that Spark completed a write cycle; they do not
 * confirm block replication or DataNode health.  For live HDFS cluster health,
 * use {@code hdfs dfsadmin -report} or the NameNode Web UI at
 * {@code http://localhost:9870}.
 *
 * <p>HDFS canonical paths:
 * <ul>
 *   <li>Raw (Parquet):       {@code /data/transactionflow/raw/transactions}</li>
 *   <li>Rejected (JSON):     {@code /data/transactionflow/rejected/transactions}</li>
 *   <li>Processed:           {@code /data/transactionflow/processed/transactions}</li>
 *   <li>Checkpoints:         {@code /data/transactionflow/checkpoints}</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/storage")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class StorageController {

    private final SparkJobMetricsRepository sparkJobMetricsRepository;

    @Value("${hdfs.namenode.url:http://hdfs-namenode:9870}")
    private String hdfsNameNodeUrl;

    @GetMapping("/hdfs/status")
    public Map<String, Object> getHdfsStatus() {
        Optional<SparkJobMetrics> latestMetrics = sparkJobMetricsRepository.findTop1ByOrderByRecordedAtDesc();

        Map<String, Object> status = new HashMap<>();

        // ── Canonical HDFS paths (must match hdfs-init, Spark job, and .env) ──
        status.put("namenodeUrl",      hdfsNameNodeUrl);
        status.put("rawPath",          "/data/transactionflow/raw/transactions");
        status.put("rejectedPath",     "/data/transactionflow/rejected/transactions");
        status.put("processedPath",    "/data/transactionflow/processed/transactions");
        status.put("checkpointPath",   "/data/transactionflow/checkpoints");
        status.put("timestamp",        Instant.now().toString());

        // ── Note: counts below are Spark-reported write metrics, not live HDFS ──
        // For live DataNode health: hdfs dfsadmin -report
        // For live file listing:   hdfs dfs -ls -R /data/transactionflow
        latestMetrics.ifPresentOrElse(metrics -> {
            status.put("sparkRawBatchesWritten",      metrics.getRawFilesWritten() != null      ? metrics.getRawFilesWritten()      : 0L);
            status.put("sparkRejectedBatchesWritten", metrics.getRejectedFilesWritten() != null ? metrics.getRejectedFilesWritten() : 0L);
            status.put("lastBatchId",      metrics.getBatchId());
            status.put("lastRecordedAt",   metrics.getRecordedAt() != null ? metrics.getRecordedAt().toString() : null);
            status.put("checkpointStatus", "ACTIVE");
            status.put("hdfsStatus",       "RUNNING");
            status.put("metricsSource",    "spark-job-metrics (PostgreSQL)");
        }, () -> {
            status.put("sparkRawBatchesWritten",      0L);
            status.put("sparkRejectedBatchesWritten", 0L);
            status.put("lastBatchId",      null);
            status.put("lastRecordedAt",   null);
            status.put("checkpointStatus", "UNKNOWN");
            status.put("hdfsStatus",       "STARTING");
            status.put("metricsSource",    "spark-job-metrics (PostgreSQL)");
        });

        return status;
    }
}
