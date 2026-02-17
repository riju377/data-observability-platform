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

  // ADD THIS: Track accumulated metrics for the entire application
  private var appMetrics: JobMetrics = JobMetrics()
  private val appInputs = mutable.Set[TableReference]()
  private val appOutputs = mutable.Set[TableReference]()
  
  // Track the job name/id for the application (from the first job or config)
  private var appJobName: Option[String] = None
  private var appApplicationId: Option[String] = None


  /**
   * Called when the application starts
   * This is an earlier hook than onJobStart, ideal for initialization
   */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    logger.info(s"Application started: ${applicationStart.appName} (ID: ${applicationStart.appId.getOrElse("unknown")})")
    appJobName = Some(applicationStart.appName)
    appApplicationId = applicationStart.appId
    ensureQueryListenerRegistered()
  }

  /**
   * Called when the application ends.
   * Publishes the final aggregated metadata for the entire application run.
   */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info("Application ending, publishing aggregated metrics")
    
    val finalMetrics = synchronized { appMetrics }
    logger.info(s"Final application metrics: $finalMetrics")
    
    // Create a composite metadata object for the entire application
    // We use "app-<appId>" as the jobId for this aggregate record
    val appMetadata = JobMetadata(
      jobId = appApplicationId.getOrElse("unknown-app"),
      jobName = appJobName,
      applicationId = appApplicationId,
      startTime = new Timestamp(applicationEnd.time), // We don't have start time handy here easily without tracking, assuming end-is-end
      endTime = Some(new Timestamp(applicationEnd.time)), 
      status = JobStatus.Success, // We assume success if we got here, or simple completion
      errorMessage = None,
      inputTables = appInputs.toSeq,
      outputTables = appOutputs.toSeq,
      metrics = finalMetrics
    )
    
    try {
      publishMetadata(appMetadata)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to publish application metrics: ${e.getMessage}")
    }
    
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

    // Priority order for job name:
    // 1. spark.observability.job.name (Custom config for multi-region/multi-tenant jobs)
    // 2. spark.job.description (Standard Spark property)
    // 3. spark.app.name (Application-level name)
    val jobName: Option[String] = Option(jobStart.properties.getProperty("spark.observability.job.name"))
                                    .orElse(Option(jobStart.properties.getProperty("spark.job.description")))
                                    .orElse(Option(jobStart.properties.getProperty("callSite.short")))
                                    .orElse {
                                      for {
                                        appName <- Option(jobStart.properties.getProperty("spark.app.name"))
                                        sqlId <- Option(jobStart.properties.getProperty("spark.sql.execution.id"))
                                      } yield s"$appName [SQL-$sqlId]"
                                    }
                                    .orElse(Option(jobStart.properties.getProperty("spark.app.name")))

    val applicationId : Option[String] = Option(jobStart.properties.getProperty("spark.app.id"))
    
    // Capture app-level info if not already set
    if (appJobName.isEmpty) appJobName = jobName
    if (appApplicationId.isEmpty) appApplicationId = applicationId

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
        // Get accumulated metrics for this job
        val finalMetrics = jobMetrics.getOrElse(jobEnd.jobId, JobMetrics())

        // Get accumulated lineage for this job
        val (inputs, outputs) = jobLineage.getOrElse(jobEnd.jobId, (Seq.empty, Seq.empty))
        
        
        // Aggregate into Application-Level Metrics and Lineage
        synchronized {
          appMetrics = combinedMetrics(appMetrics, finalMetrics)
          logger.info(s"Job ${jobEnd.jobId} metrics aggregated. Current App Metrics: $appMetrics")
          appInputs ++= inputs
          appOutputs ++= outputs
        }

        logger.debug(s"Job ${jobEnd.jobId} lineage: ${inputs.size} inputs, ${outputs.size} outputs (Added to App Aggregate)")

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
    logger.debug(s"Combining metrics: M1=$m1, M2=$m2")
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
    val metrics = stageInfo.taskMetrics

    if (metrics != null) {
      val stageMetrics = JobMetrics(
        rowsRead = Some(metrics.inputMetrics.recordsRead),
        rowsWritten = Some(metrics.outputMetrics.recordsWritten),
        bytesRead = Some(metrics.inputMetrics.bytesRead),
        bytesWritten = Some(metrics.outputMetrics.bytesWritten),
        executionTimeMs = Some(metrics.executorRunTime),
        totalTasks = Some(stageInfo.numTasks),
        failedTasks = stageInfo.failureReason.map(_ => 1).orElse(Some(0)),
        shuffleReadBytes = Some(metrics.shuffleReadMetrics.totalBytesRead),
        shuffleWriteBytes = Some(metrics.shuffleWriteMetrics.bytesWritten),
        executorCpuTimeNs = Some(metrics.executorCpuTime),
        jvmGcTimeMs = Some(metrics.jvmGCTime),
        executorDeserializeTimeMs = Some(metrics.executorDeserializeTime),
        executorDeserializeCpuTimeNs = Some(metrics.executorDeserializeCpuTime),
        resultSerializationTimeMs = Some(metrics.resultSerializationTime),
        resultSizeBytes = Some(metrics.resultSize),
        memoryBytesSpilled = Some(metrics.memoryBytesSpilled),
        diskBytesSpilled = Some(metrics.diskBytesSpilled),
        peakExecutionMemory = Some(metrics.peakExecutionMemory),
        shuffleRemoteBytesRead = Some(metrics.shuffleReadMetrics.remoteBytesRead),
        shuffleLocalBytesRead = Some(metrics.shuffleReadMetrics.localBytesRead),
        shuffleRemoteBytesReadToDisk = Some(metrics.shuffleReadMetrics.remoteBytesReadToDisk),
        shuffleFetchWaitTimeMs = Some(metrics.shuffleReadMetrics.fetchWaitTime),
        shuffleRecordsRead = Some(metrics.shuffleReadMetrics.recordsRead),
        shuffleRemoteBlocksFetched = Some(metrics.shuffleReadMetrics.remoteBlocksFetched),
        shuffleLocalBlocksFetched = Some(metrics.shuffleReadMetrics.localBlocksFetched),
        shuffleWriteTimeNs = Some(metrics.shuffleWriteMetrics.writeTime),
        shuffleRecordsWritten = Some(metrics.shuffleWriteMetrics.recordsWritten)
      )

      stageToJob.get(stageInfo.stageId) match {
        case Some(jobId) =>
          val currentMetrics = jobMetrics.getOrElse(jobId, JobMetrics())
          val updatedMetrics = combinedMetrics(currentMetrics, stageMetrics)
          jobMetrics.put(jobId, updatedMetrics)
          logger.info(s"  Stage ${stageInfo.stageId} completed. Mode: TaskMetrics. Jobs: $jobId. RowsRead: ${metrics.inputMetrics.recordsRead}")
        case None =>
          logger.warn(s"Stage ${stageInfo.stageId} not associated with any job!")
      }
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
        MetadataPublisher.publishLineageOnly(inputTables, outputTables, queryId, appJobName, sparkConf)
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
