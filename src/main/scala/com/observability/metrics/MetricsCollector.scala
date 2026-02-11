package com.observability.metrics

import com.observability.models.JobMetadata
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp

/**
 * Collects dataset-level metrics from Spark jobs
 *
 * LEARNING GOALS:
 * - Extract row counts and data sizes
 * - Track data freshness (last update time)
 * - Aggregate metrics across multiple writes
 *
 * WHY COLLECT METRICS?
 * - Detect anomalies (sudden spike/drop in row count)
 * - Monitor data quality
 * - Track dataset growth over time
 * - Identify performance issues
 */
object MetricsCollector extends LazyLogging {

  /**
   * Dataset metrics snapshot
   */
  case class DatasetMetrics(
    datasetName: String,
    rowCount: Option[Long],
    sizeBytes: Option[Long],
    lastUpdated: Timestamp,
    jobId: String,
    status: String
  )

  /**
   * Extract metrics from completed job
   *
   * WHAT WE EXTRACT:
   * - Row count: From job metrics (rowsWritten)
   * - Data size: From job metrics (bytesWritten)
   * - Last updated: Job end time
   *
   * @param metadata Job metadata with metrics
   * @return Metrics for each output dataset
   */
  def extractMetrics(metadata: JobMetadata): Seq[DatasetMetrics] = {
    logger.debug(s"Extracting metrics from job: ${metadata.jobId}")

    // Extract metrics for each output table
    metadata.outputTables.map { table =>
      DatasetMetrics(
        datasetName = table.name,
        rowCount = metadata.metrics.rowsWritten,
        sizeBytes = metadata.metrics.bytesWritten,
        lastUpdated = metadata.endTime.getOrElse(new Timestamp(System.currentTimeMillis())),
        jobId = metadata.jobId,
        status = metadata.status.toString
      )
    }
  }

  /**
   * Extract metrics directly from DataFrame
   *
   * This is useful when you have the DataFrame in memory
   * and want immediate metrics without waiting for job completion
   *
   * @param df DataFrame to measure
   * @param datasetName Name of the dataset
   * @return Metrics snapshot
   */
  def extractFromDataFrame(df: DataFrame, datasetName: String): DatasetMetrics = {
    logger.debug(s"Extracting metrics from DataFrame: $datasetName")

    // Count rows (triggers action)
    val rowCount = df.count()

    DatasetMetrics(
      datasetName = datasetName,
      rowCount = Some(rowCount),
      sizeBytes = None,  // Not easily available from DataFrame
      lastUpdated = new Timestamp(System.currentTimeMillis()),
      jobId = "direct-df-metrics",
      status = "Success"
    )
  }

  /**
   * Format metrics for display
   */
  def formatMetrics(metrics: DatasetMetrics): String = {
    val rowCountStr = metrics.rowCount.map(_.toString).getOrElse("unknown")
    val sizeStr = metrics.sizeBytes.map(bytes => formatBytes(bytes)).getOrElse("unknown")

    s"""Dataset Metrics: ${metrics.datasetName}
       |  Row Count:    $rowCountStr
       |  Size:         $sizeStr
       |  Last Updated: ${metrics.lastUpdated}
       |  Job ID:       ${metrics.jobId}
       |  Status:       ${metrics.status}
       |""".stripMargin
  }

  /**
   * Format bytes in human-readable form
   */
  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024) s"$bytes B"
    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.2f KB"
    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024)}%.2f MB"
    else f"${bytes / (1024.0 * 1024 * 1024)}%.2f GB"
  }
}
