package com.observability.publisher

import com.observability.client.ObservabilityApiClient
import com.observability.models.{ColumnLineageEdge, JobMetadata, TableReference, SchemaInfo, FieldInfo}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}

/**
 * Publishes observability data to the backend API asynchronously.
 *
 * All API calls are fire-and-forget on a dedicated thread pool
 * to avoid blocking Spark's event processing thread.
 *
 * CONFIGURATION:
 *   spark.observability.api.url = "https://observability.company.com"
 *   spark.observability.api.key = "obs_live_xxx..."
 */
object MetadataPublisher extends LazyLogging {

  // Daemon thread factory so the JVM can exit even if pending calls remain.
  // Without daemon threads, the pool keeps the JVM alive after main() completes,
  // preventing Spark's shutdown hook (which calls onApplicationEnd → shutdown()) from firing.
  private val daemonThreadFactory: ThreadFactory = new ThreadFactory {
    private val delegate = Executors.defaultThreadFactory()
    override def newThread(r: Runnable): Thread = {
      val t = delegate.newThread(r)
      t.setDaemon(true)
      t.setName(s"observability-publisher-${t.getId}")
      t
    }
  }

  // Dedicated thread pool for async API calls (2 threads is sufficient for fire-and-forget HTTP)
  private val executor: ExecutorService = Executors.newFixedThreadPool(2, daemonThreadFactory)
  private implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)

  // Cached API client (initialized on first publish call, thread-safe)
  @volatile private var cachedClient: Option[ObservabilityApiClient] = None
  @volatile private var clientInitialized = false

  /**
   * Get or initialize the API client (thread-safe, cached)
   */
  private def getClient(sparkConf: Option[SparkConf]): Option[ObservabilityApiClient] = {
    if (!clientInitialized) {
      synchronized {
        if (!clientInitialized) {
          cachedClient = sparkConf.flatMap(ObservabilityApiClient.fromSparkConf)
          clientInitialized = true
          if (cachedClient.isEmpty) {
            logger.warn("No API client configured - observability data will not be published. " +
              "Set spark.observability.api.url and spark.observability.api.key")
          }
        }
      }
    }
    cachedClient
  }

  /**
   * Publish table-level lineage (async fire-and-forget)
   *
   * Called immediately when lineage is detected in onSuccess().
   */
  def publishLineageOnly(
    inputs: Seq[TableReference],
    outputs: Seq[TableReference],
    queryId: String = "query-" + java.util.UUID.randomUUID().toString.take(8),
    jobName: Option[String] = None,
    sparkConf: Option[SparkConf] = None
  ): Unit = {
    // Only skip if there are no outputs. Inputs may be empty for
    // in-memory-to-table writes (e.g. Seq.toDF.saveAsTable) — we still
    // need to register the output dataset via the API.
    if (outputs.isEmpty) return

    getClient(sparkConf) match {
      case Some(apiClient) =>
        Future {
          apiClient.publishLineage(inputs, outputs, queryId, jobName) match {
            case Success(_) =>
              logger.debug(s"Lineage published for query: $queryId")
            case Failure(e) =>
              logger.error(s"Failed to publish lineage: ${e.getMessage}")
          }
        }
      case None =>
    }
  }

  /**
   * Publish complete job metadata including metrics (async fire-and-forget)
   */
  def publish(metadata: JobMetadata, sparkConf: Option[SparkConf] = None): Unit = {
    getClient(sparkConf) match {
      case Some(apiClient) =>
        Future {
          // Log the metadata being published (at least the ID and name)
          logger.info(s"Publishing schema/metadata for job: ${metadata.jobId}, Name: ${metadata.jobName.getOrElse("unknown")}")
          logger.info(s"Metadata payload metrics: ${metadata.metrics}")
          
          apiClient.publishMetadata(metadata) match {
            case Success(_) =>
              logger.debug(s"Metadata published for job: ${metadata.jobId}")
            case Failure(e) =>
              logger.error(s"Failed to publish metadata: ${e.getMessage}")
          }
        }
      case None =>
        logger.warn("Skipping metadata publish - no API client available")
    }
  }

  /**
   * Publish schema version (async fire-and-forget)
   */
  def publishSchemaVersion(
    datasetName: String,
    schemaSnapshot: com.observability.schema.SchemaTracker.SchemaSnapshot,
    schemaJson: String,
    sparkConf: Option[SparkConf] = None
  ): Unit = {
    getClient(sparkConf) match {
      case Some(apiClient) =>
        Future {
          val fields = schemaSnapshot.fields.map { f =>
            FieldInfo(f.name, f.dataType, f.nullable, Map.empty)
          }
          val schemaInfo = SchemaInfo(fields, schemaSnapshot.schemaHash)
          val jobId = "schema-capture-" + schemaSnapshot.schemaHash.take(8)

          apiClient.publishSchema(datasetName, schemaInfo, jobId) match {
            case Success(_) =>
              logger.debug(s"Schema published for: $datasetName")
            case Failure(e) =>
              logger.error(s"Failed to publish schema for $datasetName: ${e.getMessage}")
          }
        }
      case None =>
    }
  }

  /**
   * Publish column-level lineage (async fire-and-forget)
   */
  def publishColumnLineage(
    edges: Seq[ColumnLineageEdge],
    jobId: String = "column-lineage-" + java.util.UUID.randomUUID().toString.take(8),
    jobName: String = "column-lineage-capture",
    sparkConf: Option[SparkConf] = None
  ): Unit = {
    if (edges.isEmpty) return

    getClient(sparkConf) match {
      case Some(apiClient) =>
        Future {
          apiClient.publishColumnLineage(edges, jobId, jobName) match {
            case Success(_) =>
              logger.debug(s"Column lineage published: ${edges.size} edges for job: $jobId")
            case Failure(e) =>
              logger.error(s"Failed to publish column lineage: ${e.getMessage}")
          }
        }
      case None =>
    }
  }

  /**
   * Graceful shutdown - waits for pending API calls to complete.
   * Called from ObservabilityListener.onApplicationEnd().
   */
  def shutdown(): Unit = {
    logger.info("Shutting down async publisher, draining pending calls...")
    executor.shutdown()
    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
      val dropped = executor.shutdownNow().size()
      logger.warn(s"Publisher did not drain in 30s, dropped $dropped pending calls")
    } else {
      logger.debug("Async publisher shut down cleanly")
    }
  }
}
