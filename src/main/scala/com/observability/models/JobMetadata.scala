package com.observability.models

import java.sql.Timestamp

/**
 * Metadata captured from a Spark job execution
 *
 * This case class represents all the information we extract from a running Spark job.
 * It's the core data model that flows through our observability platform.
 */
case class JobMetadata(
  jobId: String,                        // Spark job ID
  jobName: Option[String],              // Human-readable job name (if provided)
  applicationId: Option[String],        // Spark application ID
  startTime: Timestamp,
  endTime: Option[Timestamp],
  status: JobStatus,

  // Lineage information
  inputTables: Seq[TableReference],     // Tables/files read by this job
  outputTables: Seq[TableReference],    // Tables/files written by this job

  // Metrics
  metrics: JobMetrics,

  // Error information (if failed)
  errorMessage: Option[String] = None
)

/**
 * Reference to a table or dataset
 */
case class TableReference(
  name: String,                         // Fully qualified name (e.g., "bronze.taxi_trips")
  tableType: TableType,                 // TABLE, VIEW, FILE, etc.
  location: Option[String],             // Physical location (S3 path, table name, etc.)
  schema: Option[SchemaInfo] = None     // Schema information (captured at read/write time)
)

/**
 * Schema information for a table
 */
case class SchemaInfo(
  fields: Seq[FieldInfo],
  schemaHash: String                    // Hash for quick comparison
) {
  /**
   * Generate a hash of the schema for quick comparison
   * Two schemas with the same structure will have the same hash
   */
  def computeHash(): String = {
    val schemaString = fields.map(f => s"${f.name}:${f.dataType}:${f.nullable}").mkString(",")
    java.security.MessageDigest
      .getInstance("SHA-256")
      .digest(schemaString.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString
  }
}

case class FieldInfo(
  name: String,
  dataType: String,
  nullable: Boolean,
  metadata: Map[String, String] = Map.empty
)

/**
 * Execution metrics for a job
 */
case class JobMetrics(
  // Volume metrics
  rowsRead: Option[Long] = None,
  rowsWritten: Option[Long] = None,
  bytesRead: Option[Long] = None,
  bytesWritten: Option[Long] = None,

  // Execution metrics
  executionTimeMs: Option[Long] = None,
  totalTasks: Option[Int] = None,
  failedTasks: Option[Int] = None,

  // Shuffle metrics (basic - kept for backwards compat)
  shuffleReadBytes: Option[Long] = None,
  shuffleWriteBytes: Option[Long] = None,

  // CPU utilization
  executorCpuTimeNs: Option[Long] = None,
  jvmGcTimeMs: Option[Long] = None,
  executorDeserializeTimeMs: Option[Long] = None,
  executorDeserializeCpuTimeNs: Option[Long] = None,
  resultSerializationTimeMs: Option[Long] = None,
  resultSizeBytes: Option[Long] = None,

  // Memory / Spill
  memoryBytesSpilled: Option[Long] = None,
  diskBytesSpilled: Option[Long] = None,
  peakExecutionMemory: Option[Long] = None,

  // Detailed shuffle read
  shuffleRemoteBytesRead: Option[Long] = None,
  shuffleLocalBytesRead: Option[Long] = None,
  shuffleRemoteBytesReadToDisk: Option[Long] = None,
  shuffleFetchWaitTimeMs: Option[Long] = None,
  shuffleRecordsRead: Option[Long] = None,
  shuffleRemoteBlocksFetched: Option[Long] = None,
  shuffleLocalBlocksFetched: Option[Long] = None,

  // Detailed shuffle write
  shuffleWriteTimeNs: Option[Long] = None,
  shuffleRecordsWritten: Option[Long] = None
)

/**
 * Job execution status
 */
sealed trait JobStatus
object JobStatus {
  case object Running extends JobStatus
  case object Success extends JobStatus
  case object Failed extends JobStatus
  case object Unknown extends JobStatus

  def fromString(status: String): JobStatus = status.toLowerCase match {
    case "running" => Running
    case "success" | "succeeded" => Success
    case "failed" => Failed
    case _ => Unknown
  }
}

/**
 * Type of table/dataset
 */
sealed trait TableType
object TableType {
  case object Table extends TableType       // Regular table
  case object View extends TableType        // View
  case object TempView extends TableType    // Temporary view
  case object File extends TableType        // File-based (Parquet, CSV, etc.)
  case object Stream extends TableType      // Streaming source/sink
  case object Unknown extends TableType

  def fromString(tableType: String): TableType = tableType.toLowerCase match {
    case "table" => Table
    case "view" => View
    case "tempview" | "temp_view" => TempView
    case "file" => File
    case "stream" => Stream
    case _ => Unknown
  }
}

/**
 * Column-level lineage edge
 * Tracks how individual columns flow from source to target datasets
 */
case class ColumnLineageEdge(
  sourceDatasetName: String,
  sourceColumn: String,
  targetDatasetName: String,
  targetColumn: String,
  transformType: TransformType,
  expression: Option[String] = None
)

/**
 * Types of column transformations
 */
sealed trait TransformType {
  def name: String = this.getClass.getSimpleName.replace("$", "")
}
object TransformType {
  case object DIRECT extends TransformType      // Direct column mapping (no transformation)
  case object EXPRESSION extends TransformType  // Calculated expression (e.g., col_a + col_b)
  case object AGGREGATE extends TransformType   // Aggregation (SUM, COUNT, AVG, etc.)
  case object JOIN extends TransformType        // Join key
  case object FILTER extends TransformType      // Used in WHERE/HAVING clause
  case object CASE extends TransformType        // CASE WHEN expression

  def fromString(s: String): TransformType = s.toUpperCase match {
    case "DIRECT" => DIRECT
    case "EXPRESSION" => EXPRESSION
    case "AGGREGATE" => AGGREGATE
    case "JOIN" => JOIN
    case "FILTER" => FILTER
    case "CASE" => CASE
    case _ => EXPRESSION
  }
}

/**
 * Companion object with helper methods
 */
object JobMetadata {
  /**
   * Create initial metadata when job starts
   */
  def started(
    jobId: String,
    jobName: Option[String] = None,
    applicationId: Option[String] = None
  ): JobMetadata = {
    JobMetadata(
      jobId = jobId,
      jobName = jobName,
      applicationId = applicationId,
      startTime = new Timestamp(System.currentTimeMillis()),
      endTime = None,
      status = JobStatus.Running,
      inputTables = Seq.empty,
      outputTables = Seq.empty,
      metrics = JobMetrics()
    )
  }

  /**
   * Update metadata when job completes
   */
  def completed(
    metadata: JobMetadata,
    success: Boolean,
    errorMessage: Option[String] = None
  ): JobMetadata = {
    metadata.copy(
      endTime = Some(new Timestamp(System.currentTimeMillis())),
      status = if (success) JobStatus.Success else JobStatus.Failed,
      errorMessage = errorMessage
    )
  }
}