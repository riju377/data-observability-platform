package com.observability.listener

import com.observability.models._
import com.observability.lineage.{ColumnLineageExtractor, QueryPlanParser}
import com.observability.publisher.MetadataPublisher
import com.observability.schema.SchemaTracker
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.{SparkConf, SparkEnv}

import java.sql.Timestamp
import scala.collection.mutable

/**
 * Custom Spark Listener that captures metadata from running Spark jobs
 *
 * This is the core component that makes observability work. It hooks into Spark's
 * execution engine and intercepts events like job start, job end, query execution, etc.
 *
 * LEARNING NOTE:
 * Spark has two main listener interfaces:
 * 1. SparkListener - Low-level events (jobs, stages, tasks)
 * 2. QueryExecutionListener - High-level SQL/DataFrame events
 *
 * We use both to get complete coverage.
 *
 * Interview talking point:
 * "I extended both SparkListener and QueryExecutionListener to capture metadata
 * at different levels. SparkListener gives me job-level metrics, while
 * QueryExecutionListener gives me access to the logical plan for lineage extraction."
 *
 * EMR DEPLOYMENT:
 * This listener reads database configuration from Spark properties, making it
 * easy to deploy on EMR without code changes. Configure via spark-defaults.conf
 * or --conf flags.
 */
class ObservabilityListener extends SparkListener with QueryExecutionListener with LazyLogging {

  /**
   * Get SparkConf from SparkEnv
   *
   * This allows us to read database configuration from Spark properties
   * set via --conf or spark-defaults.conf (EMR deployment pattern)
   */
  private lazy val sparkConf: Option[SparkConf] = {
    try {
      Option(SparkEnv.get).map(_.conf)
    } catch {
      case _: Exception =>
        logger.warn("SparkEnv not available, using default database configuration")
        None
    }
  }

  // Track active jobs (jobId -> metadata)
  private val activeJobs = mutable.Map[Int, JobMetadata]()

  // ADD THIS: Track which stages belong to which job
  private val stageToJob = mutable.Map[Int, Int]()  // stageId -> jobId

  // ADD THIS: Track accumulated metrics per job
  private val jobMetrics = mutable.Map[Int, JobMetrics]()  // jobId -> aggregated metrics

  // Track query executions (for lineage)
  private val queryExecutions = mutable.Map[Long, QueryExecution]()

  // Track lineage per job (accumulated from all queries in the job)
  private val jobLineage = mutable.Map[Int, (Seq[TableReference], Seq[TableReference])]()  // jobId -> (inputs, outputs)

  // Track SQL execution ID to job ID mapping (for correlating onSuccess with onJobEnd)
  private val sqlExecutionToJob = mutable.Map[String, Int]()  // sqlExecutionId -> jobId

  // NEW: Track metrics per output table (per query, not per job)
  // This allows us to attribute correct row counts to each output in multi-output jobs
  private val tableMetrics = mutable.Map[String, (Long, Long)]()  // tableName -> (rowCount, byteSize)

  // Track if we've already registered as QueryExecutionListener (do it only once)
  @volatile private var queryListenerRegistered = false

  /**
   * Self-register as QueryExecutionListener
   *
   * CRITICAL: spark.sql.queryExecutionListeners config does NOT work for programmatic registration.
   * We must call spark.listenerManager.register() explicitly.
   *
   * This method is called from onJobStart to ensure QueryExecutionListener registration
   * happens automatically without requiring user code changes.
   *
   * NOTE: We use reflection to find all SparkSession instances because getActiveSession
   * doesn't work reliably in listener context.
   */
  private def ensureQueryListenerRegistered(): Unit = {
    if (!queryListenerRegistered) {
      synchronized {
        if (!queryListenerRegistered) {
          try {
            // Try method 1: getActiveSession
            var sparkSessionOpt = org.apache.spark.sql.SparkSession.getActiveSession

            // Method 2: If that fails, use reflection to access all sessions
            if (sparkSessionOpt.isEmpty) {
              try {
                val builderClass = Class.forName("org.apache.spark.sql.SparkSession$")
                val builderModule = builderClass.getField("MODULE$").get(null)
                val getActiveSessionMethod = builderClass.getMethod("getActiveSession")
                sparkSessionOpt = getActiveSessionMethod.invoke(builderModule).asInstanceOf[Option[org.apache.spark.sql.SparkSession]]

                // If still empty, try to get default session
                if (sparkSessionOpt.isEmpty) {
                  val getDefaultSessionMethod = builderClass.getMethod("getDefaultSession")
                  sparkSessionOpt = getDefaultSessionMethod.invoke(builderModule).asInstanceOf[Option[org.apache.spark.sql.SparkSession]]
                }
              } catch {
                case e: Exception =>
                  logger.debug(s"Reflection approach failed: ${e.getMessage}")
              }
            }

            if (sparkSessionOpt.isDefined) {
              val spark = sparkSessionOpt.get
              spark.listenerManager.register(this)
              queryListenerRegistered = true
              logger.info("Self-registered as QueryExecutionListener")
            } else {
              logger.warn("No SparkSession found, will retry on next job")
            }
          } catch {
            case e: Exception =>
              logger.error(s"Failed to self-register as QueryExecutionListener: ${e.getMessage}", e)
          }
        }
      }
    }
  }

  /**
   * Called when the application starts
   * This is an earlier hook than onJobStart, ideal for initialization
   */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    logger.info(s"Application started: ${applicationStart.appName}")
    // Try to register as early as possible
    ensureQueryListenerRegistered()
  }

  /**
   * Called when the application ends.
   * Drains any pending async API calls before the JVM exits.
   */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info("Application ending, draining pending publish calls")
    MetadataPublisher.shutdown()
  }

  /**
   * Called when a Spark job starts
   *
   * This is our first hook - we create initial metadata and store it.
   */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // CRITICAL: Self-register as QueryExecutionListener on first job
    ensureQueryListenerRegistered()

    val jobName: Option[String] = Option(jobStart.properties.getProperty("spark.job.description"))
                                    .orElse(Option(jobStart.properties.getProperty("callSite.short")))
                                    .orElse {
                                      for {
                                        appName <- Option(jobStart.properties.getProperty("spark.app.name"))
                                        sqlId <- Option(jobStart.properties.getProperty("spark.sql.execution.id"))
                                      } yield s"$appName [SQL-$sqlId]"
                                    }
                                    .orElse(Option(jobStart.properties.getProperty("spark.app.name")))

    val applicationId : Option[String] = Option(jobStart.properties.getProperty("spark.app.id"))

    logger.info(s"Job ${jobStart.jobId} started: ${jobName.getOrElse("unnamed")}")

    // Create initial metadata
    val metadata = JobMetadata(
      jobId = jobStart.jobId.toString,
      jobName = jobName,
      applicationId = applicationId,
      startTime = new Timestamp(jobStart.time),
      endTime = None,
      status = JobStatus.Running,
      inputTables = Seq.empty,
      outputTables = Seq.empty,
      metrics = JobMetrics()
    )

    // Store in our tracking map
    activeJobs.put(jobStart.jobId, metadata)

    jobStart.stageIds.foreach(stageId => {
      stageToJob.put(stageId, jobStart.jobId)
    })

    jobMetrics.put(jobStart.jobId, JobMetrics())

    // Track SQL execution ID → job ID mapping (for lineage correlation)
    Option(jobStart.properties.getProperty("spark.sql.execution.id")).foreach { sqlExecId =>
      sqlExecutionToJob.put(sqlExecId, jobStart.jobId)
      logger.debug(s"Mapped SQL execution $sqlExecId → Job ${jobStart.jobId}")
    }

    // Initialize empty lineage for this job
    jobLineage.put(jobStart.jobId, (Seq.empty, Seq.empty))

    logger.debug(s"Tracking job: ${jobStart.jobId}")
  }

  /**
   * Called when a Spark job completes (success or failure)
   *
   * This is where we finalize metadata and publish it.
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {

    activeJobs.get(jobEnd.jobId) match {
      case Some(metadata) =>
        // Determine if job succeeded
        // Note: JobFailed is private in Spark 3.5, so we check for JobSucceeded
        val (success, errorMessage) = jobEnd.jobResult match {
          case JobSucceeded => (true, None)
          case _ => (false, Some(s"Job failed: ${jobEnd.jobResult.toString}"))
        }

        // Get accumulated metrics for this job
        val finalMetrics = jobMetrics.getOrElse(jobEnd.jobId, JobMetrics())

        // Get accumulated lineage for this job
        val (inputs, outputs) = jobLineage.getOrElse(jobEnd.jobId, (Seq.empty, Seq.empty))

        logger.debug(s"Job ${jobEnd.jobId} lineage: ${inputs.size} inputs, ${outputs.size} outputs")

        // Update metadata with metrics AND lineage
        val completedMetadata = metadata.copy(
          endTime = Some(new Timestamp(jobEnd.time)),
          status = if (success) JobStatus.Success else JobStatus.Failed,
          errorMessage = errorMessage,
          metrics = finalMetrics,
          inputTables = inputs,
          outputTables = outputs
        )


        // Publish metadata + metrics to backend API (async, non-blocking)
        publishMetadata(completedMetadata)

        // Clean up
        activeJobs.remove(jobEnd.jobId)
        jobMetrics.remove(jobEnd.jobId)
        jobLineage.remove(jobEnd.jobId)

        // Clean up stage mappings
        stageToJob.filter(_._2 == jobEnd.jobId).keys.foreach(stageToJob.remove)

      case None =>
        logger.warn(s"Job ${jobEnd.jobId} ended but was not tracked")
    }
  }

  private def getAccumulatorValue(
                                   accumulables: scala.collection.Map[Long, AccumulableInfo],  // More generic
                                   name: String
                                 ): Option[Long] = {
    accumulables.values
      .find(_.name.exists(_.contains(name)))
      .flatMap(_.value)
      .flatMap {
        case v: java.lang.Long => Some(v.toLong)
        case v: java.lang.Integer => Some(v.toLong)
        case v: Long => Some(v)
        case v: Int => Some(v.toLong)
        case v: String => scala.util.Try(v.toLong).toOption
        case _ => None
      }
  }

  private def sumOpt(a: Option[Long], b: Option[Long]): Option[Long] =
    (a, b) match {
      case (Some(x), Some(y)) => Some(x + y)
      case (Some(x), None) => Some(x)
      case (None, Some(y)) => Some(y)
      case _ => None
    }

  private def sumOptInt(a: Option[Int], b: Option[Int]): Option[Int] =
    (a, b) match {
      case (Some(x), Some(y)) => Some(x + y)
      case (Some(x), None) => Some(x)
      case (None, Some(y)) => Some(y)
      case _ => None
    }

  private def maxOpt(a: Option[Long], b: Option[Long]): Option[Long] =
    (a, b) match {
      case (Some(x), Some(y)) => Some(math.max(x, y))
      case (Some(x), None) => Some(x)
      case (None, Some(y)) => Some(y)
      case _ => None
    }

  private def combinedMetrics(m1: JobMetrics, m2: JobMetrics): JobMetrics = {
    JobMetrics(
      // Volume metrics
      rowsRead = sumOpt(m1.rowsRead, m2.rowsRead),
      rowsWritten = sumOpt(m1.rowsWritten, m2.rowsWritten),
      bytesRead = sumOpt(m1.bytesRead, m2.bytesRead),
      bytesWritten = sumOpt(m1.bytesWritten, m2.bytesWritten),

      // Execution metrics
      executionTimeMs = sumOpt(m1.executionTimeMs, m2.executionTimeMs),
      totalTasks = sumOptInt(m1.totalTasks, m2.totalTasks),
      failedTasks = sumOptInt(m1.failedTasks, m2.failedTasks),

      // Shuffle basics
      shuffleReadBytes = sumOpt(m1.shuffleReadBytes, m2.shuffleReadBytes),
      shuffleWriteBytes = sumOpt(m1.shuffleWriteBytes, m2.shuffleWriteBytes),

      // CPU utilization
      executorCpuTimeNs = sumOpt(m1.executorCpuTimeNs, m2.executorCpuTimeNs),
      jvmGcTimeMs = sumOpt(m1.jvmGcTimeMs, m2.jvmGcTimeMs),
      executorDeserializeTimeMs = sumOpt(m1.executorDeserializeTimeMs, m2.executorDeserializeTimeMs),
      executorDeserializeCpuTimeNs = sumOpt(m1.executorDeserializeCpuTimeNs, m2.executorDeserializeCpuTimeNs),
      resultSerializationTimeMs = sumOpt(m1.resultSerializationTimeMs, m2.resultSerializationTimeMs),
      resultSizeBytes = sumOpt(m1.resultSizeBytes, m2.resultSizeBytes),

      // Memory / Spill
      memoryBytesSpilled = sumOpt(m1.memoryBytesSpilled, m2.memoryBytesSpilled),
      diskBytesSpilled = sumOpt(m1.diskBytesSpilled, m2.diskBytesSpilled),
      peakExecutionMemory = maxOpt(m1.peakExecutionMemory, m2.peakExecutionMemory),

      // Detailed shuffle read
      shuffleRemoteBytesRead = sumOpt(m1.shuffleRemoteBytesRead, m2.shuffleRemoteBytesRead),
      shuffleLocalBytesRead = sumOpt(m1.shuffleLocalBytesRead, m2.shuffleLocalBytesRead),
      shuffleRemoteBytesReadToDisk = sumOpt(m1.shuffleRemoteBytesReadToDisk, m2.shuffleRemoteBytesReadToDisk),
      shuffleFetchWaitTimeMs = sumOpt(m1.shuffleFetchWaitTimeMs, m2.shuffleFetchWaitTimeMs),
      shuffleRecordsRead = sumOpt(m1.shuffleRecordsRead, m2.shuffleRecordsRead),
      shuffleRemoteBlocksFetched = sumOpt(m1.shuffleRemoteBlocksFetched, m2.shuffleRemoteBlocksFetched),
      shuffleLocalBlocksFetched = sumOpt(m1.shuffleLocalBlocksFetched, m2.shuffleLocalBlocksFetched),

      // Detailed shuffle write
      shuffleWriteTimeNs = sumOpt(m1.shuffleWriteTimeNs, m2.shuffleWriteTimeNs),
      shuffleRecordsWritten = sumOpt(m1.shuffleRecordsWritten, m2.shuffleRecordsWritten)
    )
  }


  /**
   * Called when a stage completes
   * We use this to collect execution metrics
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo

    // Calculate duration
    val duration = for {
      start <- stageInfo.submissionTime
      end <- stageInfo.completionTime
    } yield end - start

    // Extract all metrics from accumulables
    val acc = stageInfo.accumulables

    // I/O metrics
    val rowsRead = getAccumulatorValue(acc, "number of input")
    val rowsWritten = getAccumulatorValue(acc, "number of output")
    val bytesRead = getAccumulatorValue(acc, "input.bytesRead")
    val bytesWritten = getAccumulatorValue(acc, "output.bytesWritten")

    // Compute metrics
    val executorRunTime = getAccumulatorValue(acc, "executorRunTime")
    val executorCpuTime = getAccumulatorValue(acc, "executorCpuTime")
    val jvmGcTime = getAccumulatorValue(acc, "jvmGCTime")
    val executorDeserializeTime = getAccumulatorValue(acc, "executorDeserializeTime")
      .filterNot(_ => acc.values.exists(_.name.exists(_.contains("executorDeserializeCpuTime"))))
      .orElse(getAccumulatorValue(acc, "executorDeserializeTime"))
    val executorDeserializeCpuTime = getAccumulatorValue(acc, "executorDeserializeCpuTime")
    val resultSerializationTime = getAccumulatorValue(acc, "resultSerializationTime")
    val resultSize = getAccumulatorValue(acc, "resultSize")

    // Memory / Spill
    val memoryBytesSpilled = getAccumulatorValue(acc, "memoryBytesSpilled")
    val diskBytesSpilled = getAccumulatorValue(acc, "diskBytesSpilled")
    val peakExecutionMemory = getAccumulatorValue(acc, "peakExecutionMemory")

    // Shuffle read (detailed)
    val shuffleRemoteBytesRead = getAccumulatorValue(acc, "shuffle.read.remoteBytesRead")
    val shuffleLocalBytesRead = getAccumulatorValue(acc, "shuffle.read.localBytesRead")
    val shuffleRemoteBytesReadToDisk = getAccumulatorValue(acc, "shuffle.read.remoteBytesReadToDisk")
    val shuffleFetchWaitTime = getAccumulatorValue(acc, "shuffle.read.fetchWaitTime")
    val shuffleRecordsRead = getAccumulatorValue(acc, "shuffle.read.recordsRead")
    val shuffleRemoteBlocksFetched = getAccumulatorValue(acc, "shuffle.read.remoteBlocksFetched")
    val shuffleLocalBlocksFetched = getAccumulatorValue(acc, "shuffle.read.localBlocksFetched")
    val shuffleReadBytes = shuffleLocalBytesRead.map(_ + shuffleRemoteBytesRead.getOrElse(0L))
      .orElse(shuffleRemoteBytesRead)

    // Shuffle write (detailed)
    val shuffleWriteBytes = getAccumulatorValue(acc, "shuffle.write.bytesWritten")
    val shuffleWriteTime = getAccumulatorValue(acc, "shuffle.write.writeTime")
    val shuffleRecordsWritten = getAccumulatorValue(acc, "shuffle.write.recordsWritten")

    stageToJob.get(stageInfo.stageId) match {
      case Some(jobId) =>

        val stageMetrics = JobMetrics(
          rowsRead = rowsRead,
          rowsWritten = rowsWritten,
          bytesRead = bytesRead,
          bytesWritten = bytesWritten,
          executionTimeMs = executorRunTime,
          totalTasks = Some(stageInfo.numTasks),
          failedTasks = stageInfo.failureReason.map(_ => 1).orElse(Some(0)),
          shuffleReadBytes = shuffleReadBytes,
          shuffleWriteBytes = shuffleWriteBytes,
          executorCpuTimeNs = executorCpuTime,
          jvmGcTimeMs = jvmGcTime,
          executorDeserializeTimeMs = executorDeserializeTime,
          executorDeserializeCpuTimeNs = executorDeserializeCpuTime,
          resultSerializationTimeMs = resultSerializationTime,
          resultSizeBytes = resultSize,
          memoryBytesSpilled = memoryBytesSpilled,
          diskBytesSpilled = diskBytesSpilled,
          peakExecutionMemory = peakExecutionMemory,
          shuffleRemoteBytesRead = shuffleRemoteBytesRead,
          shuffleLocalBytesRead = shuffleLocalBytesRead,
          shuffleRemoteBytesReadToDisk = shuffleRemoteBytesReadToDisk,
          shuffleFetchWaitTimeMs = shuffleFetchWaitTime,
          shuffleRecordsRead = shuffleRecordsRead,
          shuffleRemoteBlocksFetched = shuffleRemoteBlocksFetched,
          shuffleLocalBlocksFetched = shuffleLocalBlocksFetched,
          shuffleWriteTimeNs = shuffleWriteTime,
          shuffleRecordsWritten = shuffleRecordsWritten
        )

        val currentMetrics = jobMetrics.getOrElse(jobId, JobMetrics())
        val updatedMetrics = combinedMetrics(currentMetrics, stageMetrics)
        jobMetrics.put(jobId, updatedMetrics)

        // Publish stage-level metrics to DB
        // DISABLED: Stage metrics insertion disabled
        /*
        try {
          MetadataPublisher.publishStageMetrics(
            jobId = jobId.toString,
            applicationId = activeJobs.get(jobId).flatMap(_.applicationId),
            stageId = stageInfo.stageId,
            stageName = Some(stageInfo.name),
            stageAttemptId = stageInfo.attemptNumber(),
            numTasks = stageInfo.numTasks,
            startedAt = stageInfo.submissionTime.map(t => new Timestamp(t)),
            endedAt = stageInfo.completionTime.map(t => new Timestamp(t)),
            durationMs = duration,
            metrics = stageMetrics,
            sparkConf = sparkConf
          )
        } catch {
          case e: Exception =>
            logger.error(s"Failed to publish stage metrics: ${e.getMessage}")
        }
        */

        logger.debug(s"  Accumulated job metrics: $updatedMetrics")

      case None =>
        logger.warn(s"Stage ${stageInfo.stageId} not associated with any job!")
    }
  }


  /**
   * Called when a SQL query execution succeeds
   *
   * This gives us access to the logical plan, which we'll parse for lineage.
   */
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val rawInputTables = QueryPlanParser.extractInputTables(qe.logical)
    val outputTables = QueryPlanParser.extractOutputTables(qe.logical)

    // Filter out flush/self-reads: when Spark writes to a path and re-reads
    // the same path, or reads from internal checkpoint/staging paths
    val outputPaths = outputTables.flatMap(_.location).map(normalizePathForComparison).toSet
    val outputNames = outputTables.map(_.name).toSet

    val inputTables = rawInputTables.filterNot { input =>
      val pathMatch = input.location.exists(loc =>
        outputPaths.contains(normalizePathForComparison(loc)))
      val nameMatch = outputNames.contains(input.name) && input.tableType == TableType.File
      val isCheckpoint = input.location.exists { loc =>
        loc.contains("/_temporary/") || loc.contains("/_checkpoint/") ||
        loc.contains("/.spark-staging-") || loc.contains("/_spark_metadata/")
      }

      if (pathMatch || nameMatch) {
        logger.debug(s"Skipping flush/self-read input: ${input.name}")
        true
      } else if (isCheckpoint) {
        logger.debug(s"Skipping checkpoint/temporary input: ${input.name}")
        true
      } else {
        false
      }
    }

    logger.info(s"onSuccess($funcName, ${durationNs / 1000000}ms): " +
      s"inputs=[${inputTables.map(_.name).mkString(",")}] -> outputs=[${outputTables.map(_.name).mkString(",")}]")

    // Accumulate lineage for job-end metadata (maps query outputs back to the job)
    // onSuccess runs on the calling thread where spark.sql.execution.id is available
    try {
      val sqlExecId = qe.sparkSession.sparkContext.getLocalProperty("spark.sql.execution.id")
      if (sqlExecId != null) {
        sqlExecutionToJob.get(sqlExecId).foreach { jobId =>
          val (existingInputs, existingOutputs) = jobLineage.getOrElse(jobId, (Seq.empty, Seq.empty))
          // Deduplicate by table name (later entries win)
          val mergedInputs = (existingInputs ++ inputTables).map(t => t.name -> t).toMap.values.toSeq
          val mergedOutputs = (existingOutputs ++ outputTables).map(t => t.name -> t).toMap.values.toSeq
          jobLineage.put(jobId, (mergedInputs, mergedOutputs))
          logger.debug(s"Updated jobLineage for job $jobId: ${mergedInputs.size} inputs, ${mergedOutputs.size} outputs")
        }
      }
    } catch {
      case e: Exception =>
        logger.debug(s"Could not correlate query to job for lineage accumulation: ${e.getMessage}")
    }

    // Generate a query ID for correlation
    val queryId = s"query-${qe.id}"

    // Publish table-level lineage IMMEDIATELY (don't wait for job end)
    if (outputTables.nonEmpty) {
      try {
        MetadataPublisher.publishLineageOnly(inputTables, outputTables, queryId, sparkConf)
      } catch {
        case e: Exception =>
          logger.error(s"Failed to publish table lineage from onSuccess: ${e.getMessage}")
      }
    }

    // COLUMN-LEVEL LINEAGE: Extract and publish column dependencies
    try {
      val columnEdges = ColumnLineageExtractor.extractColumnLineage(qe.logical)
      if (columnEdges.nonEmpty) {
        logger.debug(s"Extracted ${columnEdges.size} column lineage edges for query $queryId")
        MetadataPublisher.publishColumnLineage(columnEdges, queryId, funcName, sparkConf)
      } else {
        logger.debug("No column lineage edges extracted from this query")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to extract/publish column lineage: ${e.getMessage}", e)
        // Don't fail the job - column lineage is supplementary
    }

    // SCHEMA CAPTURE: Extract and publish schema for all input/output tables
    try {
      val allTables = (inputTables ++ outputTables).distinct
      var schemaPublishCount = 0

      allTables.foreach { tableRef =>
        tableRef.schema match {
          case Some(schemaInfo) =>
            // Convert SchemaInfo (FieldInfo) to SchemaSnapshot (SchemaField)
            val schemaFields = schemaInfo.fields.map { f =>
              SchemaTracker.SchemaField(
                name = f.name,
                dataType = f.dataType,
                nullable = f.nullable,
                comment = f.metadata.get("comment")  // Extract comment from metadata if exists
              )
            }

            val schemaSnapshot = SchemaTracker.SchemaSnapshot(
              fields = schemaFields,
              schemaHash = schemaInfo.schemaHash,
              fieldCount = schemaFields.length
            )

            // Convert to JSON
            val schemaJson = SchemaTracker.schemaToJson(schemaSnapshot)

            // Publish to database
            MetadataPublisher.publishSchemaVersion(tableRef.name, schemaSnapshot, schemaJson, sparkConf)
            schemaPublishCount += 1
            logger.debug(s"  Published schema for ${tableRef.name} (${schemaInfo.fields.size} fields, hash: ${schemaInfo.schemaHash.take(8)}...)")

          case None =>
            logger.debug(s"  No schema available for ${tableRef.name}")
        }
      }

      if (schemaPublishCount > 0) {
        logger.debug(s"Published schema for $schemaPublishCount dataset(s)")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to extract/publish schema: ${e.getMessage}", e)
    }
  }

  /**
   * Called when a SQL query execution fails
   */
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logger.error(s"Query failed: $funcName, error: ${exception.getMessage}")
  }

  /**
   * Publish metadata to backend API (async, non-blocking)
   */
  private def publishMetadata(metadata: JobMetadata): Unit = {
    logger.info(s"Job ${metadata.jobId} completed: ${metadata.status}, ${calculateDuration(metadata)}ms, " +
      s"${metadata.inputTables.size} inputs, ${metadata.outputTables.size} outputs")

    try {
      MetadataPublisher.publish(metadata, sparkConf)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to publish metadata for job ${metadata.jobId}: ${e.getMessage}")
    }
  }

  /** Normalize a path for comparison (strip trailing slashes and wildcards) */
  private def normalizePathForComparison(path: String): String = {
    path.stripSuffix("/").replaceAll("/\\*$", "").replaceAll("\\*", "")
  }

  /**
   * Helper method to calculate job duration
   */
  private def calculateDuration(metadata: JobMetadata): Long = {
    metadata.endTime match {
      case Some(end) => end.getTime - metadata.startTime.getTime
      case None => 0L
    }
  }

  /**
   * Extract input tables from logical plan
   *
   * TODO: Implement in Week 2, Day 8-10
   *
   * This is a key function for lineage tracking. We need to traverse the Spark
   * logical plan tree and find all "Scan" operations (table reads).
   *
   * Spark plan nodes to look for:
   * - LogicalRelation (reading from tables)
   * - HiveTableRelation (Hive tables)
   * - DataSourceV2Relation (Iceberg, Delta, etc.)
   */
  private def extractInputTables(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Seq[TableReference] = {
    // Placeholder - will implement in Week 2
    Seq.empty
  }

  /**
   * Extract output tables from logical plan
   *
   * TODO: Implement in Week 2, Day 8-10
   *
   * Look for write operations:
   * - InsertIntoHadoopFsRelationCommand
   * - CreateDataSourceTableAsSelectCommand
   * - SaveIntoDataSourceCommand
   */
  private def extractOutputTables(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Seq[TableReference] = {
    // Placeholder - will implement in Week 2
    Seq.empty
  }
}

/**
 * Companion object with helper methods
 */
object ObservabilityListener {
  /**
   * Register this listener with a SparkSession
   *
   * Usage:
   * val spark = SparkSession.builder()...
   * ObservabilityListener.register(spark)
   */
  def register(spark: org.apache.spark.sql.SparkSession): Unit = {
    val listener = new ObservabilityListener()

    // Register as SparkListener (for job events)
    spark.sparkContext.addSparkListener(listener)

    // Register as QueryExecutionListener (for SQL events)
    spark.listenerManager.register(listener)

    println("Observability listener registered")
  }
}
