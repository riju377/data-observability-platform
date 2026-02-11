package com.observability.client

import com.observability.models.{ColumnLineageEdge, JobMetadata, TableReference, SchemaInfo, FieldInfo}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf

import java.net.{HttpURLConnection, URL}
import java.io.{BufferedReader, InputStreamReader, OutputStreamWriter}
import scala.util.{Try, Success, Failure}

/**
 * HTTP Client for Observability Platform API
 * 
 * This client sends observability data to the REST API instead of
 * directly to the database. This provides:
 * - Better security (no DB credentials on Spark nodes)
 * - Centralized access control via API keys
 * - Async processing via background workers
 * - Multi-tenancy enforcement
 * 
 * CONFIGURATION:
 *   spark.observability.api.url = "https://observability.company.com"
 *   spark.observability.api.key = "obs_live_xxx..."
 * 
 * USAGE:
 *   val client = ObservabilityApiClient.fromSparkConf(sparkConf)
 *   client.publishLineage(inputs, outputs, jobId)
 */
object ObservabilityApiClient extends LazyLogging {
  
  // Config keys
  val API_URL_KEY = "spark.observability.api.url"
  val API_KEY_KEY = "spark.observability.api.key"
  val API_ENABLED_KEY = "spark.observability.api.enabled"
  val CONNECT_TIMEOUT_KEY = "spark.observability.api.connectTimeoutMs"
  val READ_TIMEOUT_KEY = "spark.observability.api.readTimeoutMs"
  
  // Defaults
  val DEFAULT_CONNECT_TIMEOUT = 5000  // 5 seconds
  val DEFAULT_READ_TIMEOUT = 10000    // 10 seconds
  
  /**
   * Create client from SparkConf
   * 
   * @return Some(client) if API is configured, None otherwise
   */
  def fromSparkConf(conf: SparkConf): Option[ObservabilityApiClient] = {
    val apiUrl = conf.getOption(API_URL_KEY)
    val apiKey = conf.getOption(API_KEY_KEY)
    val enabled = conf.getBoolean(API_ENABLED_KEY, defaultValue = true)
    
    if (!enabled) {
      logger.info("Observability API client disabled (spark.observability.api.enabled=false)")
      return None
    }
    
    (apiUrl, apiKey) match {
      case (Some(url), Some(key)) =>
        val connectTimeout = conf.getInt(CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT)
        val readTimeout = conf.getInt(READ_TIMEOUT_KEY, DEFAULT_READ_TIMEOUT)
        logger.info(s"Observability API client initialized: $url")
        Some(new ObservabilityApiClient(url, key, connectTimeout, readTimeout))
        
      case (Some(url), None) =>
        logger.warn(s"API URL configured but no API key provided (set $API_KEY_KEY)")
        None
        
      case (None, Some(_)) =>
        logger.warn(s"API key provided but no API URL (set $API_URL_KEY)")
        None
        
      case _ =>
        logger.debug("Observability API not configured, will use direct DB access")
        None
    }
  }
  
  /**
   * Check if API client is configured in SparkConf
   */
  def isConfigured(conf: SparkConf): Boolean = {
    conf.getOption(API_URL_KEY).isDefined && 
    conf.getOption(API_KEY_KEY).isDefined &&
    conf.getBoolean(API_ENABLED_KEY, defaultValue = true)
  }
}

/**
 * Instance of the API client
 */
class ObservabilityApiClient(
    val apiUrl: String,
    val apiKey: String,
    val connectTimeoutMs: Int = 5000,
    val readTimeoutMs: Int = 10000
) extends LazyLogging {
  
  /**
   * Publish lineage data (inputs, outputs, edges)
   */
  def publishLineage(
      inputs: Seq[TableReference],
      outputs: Seq[TableReference],
      jobId: String,
      jobName: Option[String] = None
  ): Try[ApiResponse] = {
    
    val lineageEdges = for {
      input <- inputs
      output <- outputs
    } yield s"""{"source": "${input.name}", "target": "${output.name}"}"""
    
    val inputsJson = inputs.map(tableRefToJson).mkString(",")
    val outputsJson = outputs.map(tableRefToJson).mkString(",")
    val edgesJson = lineageEdges.mkString(",")

    val payload = s"""{
      |  "api_version": "1.0",
      |  "job_id": "$jobId",
      |  "job_name": ${jobName.map(n => s""""$n"""").getOrElse("null")},
      |  "inputs": [$inputsJson],
      |  "outputs": [$outputsJson],
      |  "lineage_edges": [$edgesJson]
      |}""".stripMargin

    post("/api/v1/ingest/metadata", payload)
  }
  
  /**
   * Publish complete job metadata
   */
  def publishMetadata(metadata: JobMetadata): Try[ApiResponse] = {
    val inputsJson = metadata.inputTables.map(tableRefToJson).mkString(",")
    val outputsJson = metadata.outputTables.map(tableRefToJson).mkString(",")
    
    val lineageEdges = for {
      input <- metadata.inputTables
      output <- metadata.outputTables
    } yield s"""{"source": "${input.name}", "target": "${output.name}"}"""
    val edgesJson = lineageEdges.mkString(",")
    
    // Build metrics map
    val metricsJson = metadata.outputTables.map { table =>
      s""""${table.name}": {"row_count": ${metadata.metrics.rowsWritten.getOrElse(0)}}"""
    }.mkString(",")
    
    val payload = s"""{
      |  "api_version": "1.0",
      |  "job_id": "${metadata.jobId}",
      |  "job_name": ${metadata.jobName.map(n => s""""$n"""").getOrElse("null")},
      |  "application_id": ${metadata.applicationId.map(id => s""""$id"""").getOrElse("null")},
      |  "inputs": [$inputsJson],
      |  "outputs": [$outputsJson],
      |  "lineage_edges": [$edgesJson],
      |  "metrics": {$metricsJson}
      |}""".stripMargin
    
    post("/api/v1/ingest/metadata", payload)
  }
  
  /**
   * Publish column-level lineage
   */
  def publishColumnLineage(
    edges: Seq[ColumnLineageEdge],
    jobId: String,
    jobName: String
  ): Try[ApiResponse] = {
    
    val columnEdgesJson = edges.map { edge =>
      s"""{
         |  "source_table": "${edge.sourceDatasetName}",
         |  "source_column": "${edge.sourceColumn}",
         |  "target_table": "${edge.targetDatasetName}",
         |  "target_column": "${edge.targetColumn}",
         |  "transformation_type": "${edge.transformType.name}",
         |  "expression": ${edge.expression.map(e => s""""${e.replace("\"", "\\\"")}"""").getOrElse("null")}
         |}""".stripMargin
    }.mkString(",")

    val payload = s"""{
      |  "api_version": "1.0",
      |  "job_id": "$jobId",
      |  "job_name": "$jobName",
      |  "column_lineage": [$columnEdgesJson]
      |}""".stripMargin
      
    post("/api/v1/ingest/metadata", payload)
  }

  /**
   * Publish schema information
   */
  def publishSchema(
      datasetName: String,
      schema: SchemaInfo,
      jobId: String
  ): Try[ApiResponse] = {
    val fieldsJson = schema.fields.map { field =>
      s"""{"name": "${field.name}", "data_type": "${field.dataType}", "nullable": ${field.nullable}}"""
    }.mkString(",")
    
    val payload = s"""{
      |  "job_id": "$jobId",
      |  "schemas": [{
      |    "dataset_name": "$datasetName",
      |    "fields": [$fieldsJson],
      |    "version_hash": "${schema.schemaHash}"
      |  }]
      |}""".stripMargin
    
    post("/api/v1/ingest/schema", payload)
  }
  
  /** Serialize a TableReference to JSON including location */
  private def tableRefToJson(t: TableReference): String = {
    val loc = t.location.map(l => s""", "location": "$l"""").getOrElse("")
    s"""{"name": "${t.name}", "type": "${t.tableType.toString}"$loc}"""
  }

  /**
   * Make HTTP POST request to API
   */
  private def post(endpoint: String, payload: String): Try[ApiResponse] = {
    val url = new URL(apiUrl.stripSuffix("/") + endpoint)
    
    Try {
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      try {
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", "application/json")
        conn.setRequestProperty("Authorization", s"Bearer $apiKey")
        conn.setConnectTimeout(connectTimeoutMs)
        conn.setReadTimeout(readTimeoutMs)
        conn.setDoOutput(true)
        
        // Write payload
        val writer = new OutputStreamWriter(conn.getOutputStream)
        writer.write(payload)
        writer.flush()
        writer.close()
        
        // Read response
        val responseCode = conn.getResponseCode
        val reader = new BufferedReader(new InputStreamReader(
          if (responseCode >= 400) conn.getErrorStream else conn.getInputStream
        ))
        val response = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
        reader.close()
        
        ApiResponse(responseCode, response)
      } finally {
        conn.disconnect()
      }
    } match {
      case Success(response) if response.statusCode >= 200 && response.statusCode < 300 =>
        logger.debug(s"API call successful: $endpoint (${response.statusCode})")
        Success(response)

      case Success(response) =>
        logger.warn(s"API call failed: $endpoint (${response.statusCode}): ${response.body}")
        Failure(new ApiException(response.statusCode, response.body))

      case Failure(e) =>
        logger.error(s"API call error: $endpoint - ${e.getMessage}")
        Failure(e)
    }
  }
}

/**
 * API response wrapper
 */
case class ApiResponse(statusCode: Int, body: String) {
  def isSuccess: Boolean = statusCode >= 200 && statusCode < 300
}

/**
 * API exception for non-2xx responses
 */
class ApiException(val statusCode: Int, val body: String) 
  extends Exception(s"API error ($statusCode): $body")
