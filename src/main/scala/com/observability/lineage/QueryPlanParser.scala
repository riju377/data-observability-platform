package com.observability.lineage

import com.observability.models.{TableReference, TableType, SchemaInfo, FieldInfo}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.util.matching.Regex


/**
 * Parses Spark logical plans to extract data lineage (input/output tables)
 *
 * LEARNING GOALS:
 * - Understand how Spark represents query plans as trees
 * - Practice recursive tree traversal
 * - Use pattern matching to handle different node types
 * - Extract table names from Spark internal structures
 *
 * WEEK 2 IMPLEMENTATION - You will implement the TODO sections
 */
object QueryPlanParser extends LazyLogging {

  /**
   * Extract all INPUT tables from a logical plan
   *
   * INPUT tables are sources of data - tables we READ from
   *
   * Look for these node types:
   * - LogicalRelation: Reading from DataSource tables
   * - SubqueryAlias: Contains table name
   * - Relation: Physical table with schema
   *
   * Example plan:
   *   SubqueryAlias spark_catalog.default.bronze_taxi_trips
   *      +- Relation spark_catalog.default.bronze_taxi_trips[...] parquet
   *
   * Should extract: "bronze_taxi_trips"
   *
   * @param plan The logical plan to analyze
   * @return Sequence of input table references
   */
  def extractInputTables(plan: LogicalPlan): Seq[TableReference] = {
    val allNodes = traversePlan(plan)

    val inputTables = allNodes
      .flatMap(extractInputFromNode)
      .distinct

    logger.debug(s"Extracted ${inputTables.size} input tables: ${inputTables.map(_.name).mkString(", ")}")
    inputTables
  }

  /**
   * Extract all OUTPUT tables from a logical plan
   *
   * OUTPUT tables are targets - tables we WRITE to
   *
   * Look for these node types:
   * - CreateDataSourceTableAsSelectCommand: CREATE TABLE AS SELECT
   * - InsertIntoHadoopFsRelationCommand: INSERT/WRITE operations
   *
   * Example plan:
   *   CreateDataSourceTableAsSelectCommand `spark_catalog`.`default`.`silver_enriched_trips`, Overwrite
   *      +- Project [...]
   *
   * Should extract: "silver_enriched_trips"
   *
   * @param plan The logical plan to analyze
   * @return Sequence of output table references
   */
  def extractOutputTables(plan: LogicalPlan): Seq[TableReference] = {
    val allNodes = traversePlan(plan)

    val outputTables = allNodes
      .flatMap(extractOutputFromNode)
      .distinct

    logger.debug(s"Extracted ${outputTables.size} output tables: ${outputTables.map(_.name).mkString(", ")}")
    outputTables
  }

  /**
   * Recursively traverse the logical plan tree and collect all nodes
   *
   * This is a DEPTH-FIRST traversal:
   * - Visit current node
   * - Recursively visit all children
   * - Flatten results into single list
   *
   * LEARNING: This is a classic tree traversal algorithm
   *
   * @param node Current node to traverse
   * @return Flat sequence of all nodes in the tree
   */
  private def traversePlan(node: LogicalPlan): Seq[LogicalPlan] = {
    // TODO: Implement recursive traversal
    // HINT: node +: node.children.flatMap(child => traversePlan(child))
    //
    // EXPLANATION:
    // - node +: ... = Prepend current node to the list
    // - node.children = Get all child nodes (returns Seq[LogicalPlan])
    // - .flatMap(child => traversePlan(child)) = Recursively visit each child and flatten results
    //
    // WHY flatMap instead of map?
    // - map would give us Seq[Seq[LogicalPlan]] (nested lists)
    // - flatMap gives us Seq[LogicalPlan] (flat list)

    val className = node.getClass.getSimpleName

    // Special handling for write command nodes that have a 'query' field
    // V1 commands: InsertIntoHadoopFsRelationCommand, CreateDataSourceTableAsSelectCommand
    // V2 commands: CreateTableAsSelect, ReplaceTableAsSelect, AppendData, OverwriteByExpression
    val queryChildren = try {
      val queryMethod = node.getClass.getMethod("query")
      val queryPlan = queryMethod.invoke(node).asInstanceOf[LogicalPlan]
      Seq(queryPlan)
    } catch {
      case _: Exception => Seq.empty[LogicalPlan]
    }

    // Traverse both regular children AND query children from command nodes
    val allChildren = node.children ++ queryChildren
    node +: allChildren.flatMap(child => traversePlan(child))
  }

  /**
   * Try to extract an INPUT table from a single plan node
   *
   * Uses pattern matching to handle different node types
   *
   * @param node The plan node to analyze
   * @return Some(TableReference) if this node represents an input table, None otherwise
   */
  private def extractInputFromNode(node: LogicalPlan): Option[TableReference] = {
    // TODO: Implement pattern matching
    //
    // PATTERN 1: SubqueryAlias (most common for table reads)
    // - Check if node is SubqueryAlias
    // - Extract: alias.identifier.name (gets "bronze_taxi_trips" from "spark_catalog.default.bronze_taxi_trips")
    // - Create TableReference with name, TableType.Table, location = None
    //
    // PATTERN 2: LogicalRelation (DataSource tables)
    // - Check if node is LogicalRelation
    // - Extract: relation.catalogTable.map(_.identifier.table)
    // - Create TableReference if catalogTable exists
    //
    // PATTERN 3: Skip LocalRelation (in-memory data, not a real table)
    // - Check class name: node.getClass.getSimpleName == "LocalRelation"
    // - Return None
    //
    // DEFAULT: Return None for other node types

    val className = node.getClass.getSimpleName
    logger.debug(s"Checking node type: $className")

    // Skip LocalRelation (in-memory data, not a real table)
    if (className.contains("LocalRelation")) {
      logger.debug("Skipping LocalRelation (in-memory data)")
      return None
    }

    // Pattern 1: SubqueryAlias - Most common for table reads
    // We use reflection to avoid importing internal classes
    if (className == "SubqueryAlias") {
      try {
        // Use reflection to access identifier.name
        val identifierField = node.getClass.getMethod("identifier")
        val identifier = identifierField.invoke(node)
        val nameField = identifier.getClass.getMethod("name")
        val fullName = nameField.invoke(identifier).asInstanceOf[String]

        val tableName = extractTableName(fullName)
        val schema = extractSchemaFromNode(node)
        logger.debug(s"Found SubqueryAlias: $tableName")

        return Some(TableReference(
          name = tableName,
          tableType = TableType.Table,
          location = None,
          schema = schema
        ))
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to extract table from SubqueryAlias: ${e.getMessage}")
          return None
      }
    }

    // Pattern 2: LogicalRelation - DataSource tables or file-based reads
    node match {
      case relation: LogicalRelation =>
        relation.catalogTable match {
          case Some(table) =>
            // Reading from catalog table
            val tableName = table.identifier.table
            logger.debug(s"Found LogicalRelation (catalog): $tableName")
            Some(TableReference(
              name = tableName,
              tableType = TableType.Table,
              location = None
            ))
          case None =>
            // Reading from file path (not a catalog table)
            // Try to extract path from the BaseRelation using reflection
            try {
              val baseRelation = relation.relation
              val baseRelationClass = baseRelation.getClass.getSimpleName

              // For HadoopFsRelation (Parquet, ORC, CSV, etc.)
              if (baseRelationClass.contains("HadoopFsRelation")) {
                try {
                  val locationMethod = baseRelation.getClass.getMethod("location")
                  val location = locationMethod.invoke(baseRelation)
                  val rootPathsMethod = location.getClass.getMethod("rootPaths")
                  val rootPaths = rootPathsMethod.invoke(location).asInstanceOf[Seq[_]]

                  if (rootPaths.nonEmpty) {
                    val path = rootPaths.head.toString
                    val name = extractNameFromPath(path)
                    
                    // CRITICAL FIX: Use the base relation's schema, NOT the node's output
                    // node.output might be pruned to only used columns
                    // relation.relation.schema contains the full file schema
                    val fullSchema = try {
                      val schema = relation.relation.schema
                      val fields = schema.map { field =>
                        FieldInfo(field.name, field.dataType.simpleString, field.nullable, Map.empty)
                      }
                      val sInfo = SchemaInfo(fields, "")
                      Some(sInfo.copy(schemaHash = sInfo.computeHash()))
                    } catch {
                      case e: Exception => 
                        logger.warn(s"Failed to extract full schema from HadoopFsRelation: ${e.getMessage}")
                        extractSchemaFromNode(node) // Fallback
                    }

                    logger.debug(s"Extracted file input from HadoopFsRelation: $name from $path")
                    return Some(TableReference(
                      name = name,
                      tableType = TableType.File,
                      location = Some(generalizePath(path)),
                      schema = fullSchema
                    ))
                  }
                } catch {
                  case e: Exception =>
                    logger.warn(s"Failed to extract path from HadoopFsRelation: ${e.getMessage}")
                }
              }

              // Fallback: try to extract from string representation
              val relationString = relation.toString
              logger.debug(s"Found LogicalRelation (file-based): $relationString")

              extractFilePathFromString(relationString) match {
                case Some(path) =>
                  val name = extractNameFromPath(path)
                  logger.debug(s"Parsed file input: $name from $path")
                  Some(TableReference(
                    name = name,
                    tableType = TableType.File,
                    location = Some(generalizePath(path))
                  ))
                case None =>
                  // Could be JDBC, Snowflake, BigQuery or other datasource
                  extractDatasourceTableFromString(relationString) match {
                    case Some(tableRef) => Some(tableRef)
                    case None =>
                      logger.debug(s"Could not parse datasource from LogicalRelation")
                      None
                  }
              }
            } catch {
              case e: Exception =>
                logger.warn(s"Error extracting from LogicalRelation: ${e.getMessage}")
                None
            }
        }
      case _ =>
        // Pattern 3: DataSourceV2Relation (Iceberg, Delta, etc.)
        // We use reflection/string parsing to match ANY V2 relation without importing private classes
        if (className.contains("DataSourceV2Relation")) {
           logger.debug(s"Found DataSourceV2Relation: $className")
           try {
             // V2 relations usually print as "RelationV2[...]" or similar
             // We can allow the generic toString parser to handle it, OR look for specific V2 fields
             // Often V2 relations have a 'table' field which has 'name'
             val tableField = node.getClass.getMethod("table")
             val table = tableField.invoke(node)
             val tableNameMethod = table.getClass.getMethod("name")
             val fullName = tableNameMethod.invoke(table).asInstanceOf[String]
             
             // fullName might be "spark_catalog.default.table"
             val tableName = extractTableName(fullName)
             val schema = extractSchemaFromNode(node)

             logger.debug(s"Extracted V2 table: $tableName")
             Some(TableReference(
               name = tableName,
               tableType = TableType.Table,
               location = None,
               schema = schema
             ))
           } catch {
             case e: Exception =>
               logger.warn(s"Failed to extract info from DataSourceV2Relation via reflection: ${e.getMessage}")
               // Fallback to string parsing
               extractDatasourceTableFromString(node.toString)
           }
        } else {
          None
        }
    }

  }

  /**
   * Try to extract an OUTPUT table from a single plan node
   *
   * Output commands are trickier because they're in the execution package
   * and some classes are private. We'll use class name matching.
   *
   * @param node The plan node to analyze
   * @return Some(TableReference) if this node represents an output table, None otherwise
   */
  private def extractOutputFromNode(node: LogicalPlan): Option[TableReference] = {
    val className = node.getClass.getSimpleName
    logger.debug(s"Checking output node type: $className")

    // TODO: Implement output extraction
    //
    // STRATEGY: Use className and toString() parsing
    //
    // PATTERN 1: CreateDataSourceTableAsSelectCommand
    // - Check: className.contains("CreateDataSourceTableAsSelectCommand")
    // - Parse table name from toString:
    //   Format: "CreateDataSourceTableAsSelectCommand `spark_catalog`.`default`.`table_name`, ..."
    // - Use regex or string manipulation to extract "table_name"
    //
    // PATTERN 2: InsertIntoHadoopFsRelationCommand
    // - Check: className.contains("InsertIntoHadoopFsRelationCommand")
    // - This writes to file path, harder to get table name
    // - Can try parsing from toString or skip for now
    //
    // HINT: Use this to parse table name from string:
    // val tableNamePattern = "`([^`]+)`\\.`([^`]+)`\\.`([^`]+)`".r
    // This regex matches: `catalog`.`database`.`table`

    if (className.contains("CreateDataSourceTableAsSelectCommand")) {
      val planString = node.toString
      logger.debug(s"CreateDataSourceTableAsSelectCommand planString: ${planString.take(300)}")

      // Strategy 1: Use reflection to get the table name directly from the command's 'table' field
      val reflectionResult = try {
        val tableField = node.getClass.getMethod("table")
        val catalogTable = tableField.invoke(node)  // CatalogTable
        val identField = catalogTable.getClass.getMethod("identifier")
        val ident = identField.invoke(catalogTable)  // TableIdentifier
        val tableNameMethod = ident.getClass.getMethod("table")
        val tableName = tableNameMethod.invoke(ident).asInstanceOf[String]
        val schema = extractSchemaFromWriteCommand(node)
        logger.debug(s"Extracted output table via reflection: $tableName (schema: ${schema.isDefined})")
        Some(TableReference(name = tableName, tableType = TableType.Table, location = None, schema = schema))
      } catch {
        case e: Exception =>
          logger.warn(s"Reflection failed for CreateDataSourceTableAsSelectCommand: ${e.getMessage}")
          None
      }

      if (reflectionResult.isDefined) {
        reflectionResult
      } else {
        // Strategy 2: Parse table name using regex (fallback)
        val tableNamePattern: Regex = "`([^`]+)`\\.`([^`]+)`\\.`([^`]+)`".r
        tableNamePattern.findFirstMatchIn(planString) match {
          case Some(m) =>
            val schema = extractSchemaFromWriteCommand(node)
            logger.debug(s"Parsed output table via regex: ${m.group(3)} (schema: ${schema.isDefined})")
            Some(TableReference(name = m.group(3), tableType = TableType.Table, location = None, schema = schema))
          case None =>
            // Strategy 3: Try dot-separated pattern without backticks
            val dotPattern: Regex = "(?:spark_catalog|hive_metastore)\\.(?:default|\\w+)\\.(\\w+)".r
            dotPattern.findFirstMatchIn(planString) match {
              case Some(m) =>
                val schema = extractSchemaFromWriteCommand(node)
                Some(TableReference(name = m.group(1), tableType = TableType.Table, location = None, schema = schema))
              case None =>
                logger.warn(s"Could not parse table name from: ${planString.take(300)}")
                None
            }
        }
      }
    } else if (className.contains("InsertIntoHadoopFsRelationCommand")) {
      // PATTERN 2: File-based writes (S3, HDFS, local files) or managed table overwrites
      val planString = node.toString
      logger.debug(s"Parsing file-based write: $planString")

      // First try: check if this is a catalog table write (saveAsTable on existing table)
      // InsertIntoHadoopFsRelationCommand may have a catalogTable field
      val catalogTableName = try {
        val catalogTableMethod = node.getClass.getMethod("catalogTable")
        val catalogTableOpt = catalogTableMethod.invoke(node).asInstanceOf[Option[_]]
        catalogTableOpt.map { ct =>
          val identMethod = ct.getClass.getMethod("identifier")
          val ident = identMethod.invoke(ct)
          val tableMethod = ident.getClass.getMethod("table")
          tableMethod.invoke(ident).asInstanceOf[String]
        }
      } catch {
        case _: Exception => None
      }

      catalogTableName match {
        case Some(tableName) =>
          val schema = extractSchemaFromWriteCommand(node)
          logger.debug(s"Parsed catalog table output: $tableName (schema: ${schema.isDefined})")
          Some(TableReference(
            name = tableName,
            tableType = TableType.Table,
            location = None,
            schema = schema
          ))
        case None =>
          // Fallback: extract file path
          extractFilePathFromString(planString) match {
            case Some(path) =>
              val name = extractNameFromPath(path)
              val schema = extractSchemaFromWriteCommand(node)
              logger.debug(s"Parsed file output: $name at $path (schema: ${schema.isDefined})")
              Some(TableReference(
                name = name,
                tableType = TableType.File,
                location = Some(generalizePath(path)),
                schema = schema
              ))
            case None =>
              logger.warn(s"Could not extract path from: $planString")
              None
          }
      }
    } else if (className.contains("SaveIntoDataSourceCommand")) {
      // PATTERN 3: DataFrame.save() operations
      val planString = node.toString
      logger.debug(s"Parsing SaveIntoDataSourceCommand: $planString")

      extractFilePathFromString(planString) match {
        case Some(path) =>
          val name = extractNameFromPath(path)
          val schema = extractSchemaFromWriteCommand(node)

          Some(TableReference(
            name = name,
            tableType = TableType.File,
            location = Some(generalizePath(path)),
            schema = schema
          ))
        case None => None
      }
    } else if (className.contains("CreateTableAsSelect") || className.contains("ReplaceTableAsSelect") ||
               className.contains("ReplaceTable") || className.contains("OverwriteByExpression") ||
               className.contains("AppendData") || className.contains("OverwritePartitionsDynamic")) {
      // PATTERN 4: V2 catalog write commands (Spark 3.x DataSourceV2)
      // These are used when Spark uses the V2 session catalog (default for saveAsTable in Spark 3.x)
      logger.debug(s"Parsing V2 write command: $className")
      extractV2WriteTable(node)
    } else if (className.contains("InsertIntoStatement") || className.contains("InsertIntoDataSourceCommand")) {
      // PATTERN 5: INSERT INTO statements and DataSource inserts
      val planString = node.toString
      logger.debug(s"Parsing insert command: $planString")

      val tableNamePattern: Regex = "`([^`]+)`\\.`([^`]+)`\\.`([^`]+)`".r
      tableNamePattern.findFirstMatchIn(planString) match {
        case Some(m) =>
          val schema = extractSchemaFromWriteCommand(node)
          Some(TableReference(name = m.group(3), tableType = TableType.Table, location = None, schema = schema))
        case None => None
      }
    } else {
      None
    }
  }

  /**
   * Extract table reference from V2 catalog write commands
   * Handles: CreateTableAsSelect, ReplaceTableAsSelect, OverwriteByExpression, AppendData, etc.
   */
  private def extractV2WriteTable(node: LogicalPlan): Option[TableReference] = {
    val className = node.getClass.getSimpleName
    logger.debug(s"Extracting V2 write table from: $className")

    // Strategy 1: Try 'name' method (V2 resolved tables have it)
    try {
      val nameMethod = node.getClass.getMethod("name")
      val name = nameMethod.invoke(node).asInstanceOf[Seq[String]]
      if (name.nonEmpty) {
        val tableName = name.last
        val schema = extractSchemaFromWriteCommand(node)
        logger.debug(s"V2 write: extracted table name from name(): $tableName")
        return Some(TableReference(name = tableName, tableType = TableType.Table, location = None, schema = schema))
      }
    } catch {
      case _: Exception => // Try next strategy
    }

    // Strategy 2: Try 'table' method (OverwriteByExpression, AppendData have a 'table' field)
    try {
      val tableMethod = node.getClass.getMethod("table")
      val table = tableMethod.invoke(node)
      // table might be a DataSourceV2Relation or NamedRelation
      try {
        val tableNameMethod = table.getClass.getMethod("name")
        val fullName = tableNameMethod.invoke(table).asInstanceOf[String]
        val tableName = extractTableName(fullName)
        val schema = extractSchemaFromWriteCommand(node)
        logger.debug(s"V2 write: extracted table name from table.name(): $tableName")
        return Some(TableReference(name = tableName, tableType = TableType.Table, location = None, schema = schema))
      } catch {
        case _: Exception => // Try toString
      }
    } catch {
      case _: Exception => // Try next strategy
    }

    // Strategy 3: Parse from toString (fallback)
    val planString = node.toString
    // Look for backtick-quoted identifiers: `catalog`.`db`.`table` or just `table`
    val threePartPattern: Regex = "`([^`]+)`\\.`([^`]+)`\\.`([^`]+)`".r
    val twoPartPattern: Regex = "`([^`]+)`\\.`([^`]+)`".r
    val singlePattern: Regex = "`([^`]+)`".r

    threePartPattern.findFirstMatchIn(planString) match {
      case Some(m) =>
        val schema = extractSchemaFromWriteCommand(node)
        logger.debug(s"V2 write: parsed 3-part table name: ${m.group(3)}")
        return Some(TableReference(name = m.group(3), tableType = TableType.Table, location = None, schema = schema))
      case None =>
    }

    twoPartPattern.findFirstMatchIn(planString) match {
      case Some(m) =>
        val schema = extractSchemaFromWriteCommand(node)
        logger.debug(s"V2 write: parsed 2-part table name: ${m.group(2)}")
        return Some(TableReference(name = m.group(2), tableType = TableType.Table, location = None, schema = schema))
      case None =>
    }

    // Last resort: look for identifiers like spark_catalog.default.tablename (without backticks)
    val dotPattern: Regex = "(?:spark_catalog|hive_metastore)\\.(?:default|\\w+)\\.(\\w+)".r
    dotPattern.findFirstMatchIn(planString) match {
      case Some(m) =>
        val schema = extractSchemaFromWriteCommand(node)
        logger.debug(s"V2 write: parsed dot-notation table name: ${m.group(1)}")
        Some(TableReference(name = m.group(1), tableType = TableType.Table, location = None, schema = schema))
      case None =>
        logger.warn(s"V2 write: could not extract table name from $className: ${planString.take(200)}")
        None
    }
  }

  /**
   * Extract file path from plan string
   * Handles: s3://, file://, hdfs://, etc.
   */
  private def extractFilePathFromString(planString: String): Option[String] = {
    // Match various URI schemes and local paths
    val patterns = Seq(
      "(s3[a-z]*://[^,\\s\\]]+)".r,           // S3 paths (s3, s3a, s3n)
      "(gs://[^,\\s\\]]+)".r,                 // Google Cloud Storage
      "(wasb[s]?://[^,\\s\\]]+)".r,           // Azure Blob Storage
      "(abfs[s]?://[^,\\s\\]]+)".r,           // Azure Data Lake Gen2
      "(adl://[^,\\s\\]]+)".r,                // Azure Data Lake Gen1
      "(dbfs:/[^,\\s\\]]+)".r,                // Databricks File System
      "(file:(?://)?[^,\\s\\]]+)".r,          // File URIs (file:// or file:)
      "(hdfs://[^,\\s\\]]+)".r,               // HDFS paths
      "path=([^,\\s\\]]+)".r,                 // path= parameter
      "(/[A-Za-z0-9_/.-]+)".r                 // Absolute local paths
    )

    patterns.foreach { pattern =>
      pattern.findFirstMatchIn(planString) match {
        case Some(m) =>
          val path = m.group(1).replaceAll("[,\\s\\]]+$", "")
          logger.debug(s"Extracted path: $path from pattern: ${pattern.toString}")
          return Some(path)
        case None => // Try next pattern
      }
    }
    None
  }

  /**
   * Extract a human-readable name from a file path with optional normalization
   *
   * Normalization handles partitioned datasets:
   * - "s3://bucket/data/date=2024-01-01/country=US/" => "data" (normalized)
   * - "s3://bucket/data/2024-01-01/US/" => "data" (if date/country patterns detected)
   *
   * @param path Full file path
   * @param normalize Whether to normalize partitioned paths (default: true)
   * @return Dataset name
   */
  private def extractNameFromPath(path: String, normalize: Boolean = true): String = {
    // Remove URI scheme (s3://, file://, etc.)
    val withoutScheme = path.replaceAll("^[a-z]+://", "")

    if (normalize) {
      // Try to detect and normalize partitioned paths
      normalizePartitionedPath(withoutScheme)
    } else {
      // Original behavior: just get the last part
      val parts = withoutScheme.split("/")
      val lastName = parts.lastOption.getOrElse("unknown_dataset")
      lastName.replaceAll("\\.(parquet|csv|json|orc|avro)$", "")
    }
  }

  /**
   * Generalize a file path by replacing partition values with wildcards
   *
   * Examples:
   * - /data/date=2024-01-01/country=US/file.parquet -> /data/date={wildcard}/country={wildcard}/
   * - /data/2024/01/01/data.csv -> /data/{wildcard}/{wildcard}/{wildcard}
   * - /data/US/agg -> /data/{wildcard}/agg
   */

  private def generalizePath(path: String): String = {
    // Split into scheme and path
    val (scheme, pathPart) = if (path.contains("://")) {
      val parts = path.split("://", 2)
      (parts(0) + "://", parts(1))
    } else if (path.startsWith("file:")) {
      ("file:", path.substring(5)) // Handle file:/path
    } else {
      ("", path)
    }

    val parts = pathPart.split("/")
    
    // Patterns to match dynamic segments
    val patterns = Seq(
      // Key=Value patterns (keep key, wildcard value)
      "^(date|year|month|day|hour|dt|partition_date)=.+".r -> "$1=*",
      "^(country|region|zone|geo|location|market)=.+".r -> "$1=*",
      
      // Value-only patterns (full wildcard)
      "^\\d{4}-\\d{2}-\\d{2}$".r -> "*",           // 2024-01-01
      "^\\d{8}$".r -> "*",                         // 20240101
      "^\\d{4}$".r -> "*",                         // 2024 (Year)
      "^(0[1-9]|1[0-2])$".r -> "*",                // 01-12 (Month)
      "^(0[1-9]|[12][0-9]|3[01])$".r -> "*",       // 01-31 (Day)
      "^[A-Z]{2}$".r -> "*",                       // US, UK (Country Code)
      "^[a-z]{2}-[a-z]+-\\d+$".r -> "*",           // us-east-1 (Region)
      "^(staging|prod|dev|test)$".r -> "*"         // Environment
    )

    val generalizedParts = parts.map { part =>
      patterns.find(_._1.findFirstIn(part).isDefined) match {
        case Some((_, replacement)) => 
           if (replacement.contains("$1")) {
             // Extract key from part (key=value)
             part.split("=").head + "=*"
           } else {
             replacement
           }
        case None => part
      }
    }

    // Reconstruct path
    // Verify if original path started with / (for local paths)
    val prefix = if (pathPart.startsWith("/")) "/" else ""
    val generalizedPath = prefix + generalizedParts.mkString("/")
    
    // Remove "file:" prefix if it was split out, keeping standard format
    if (scheme == "file:") "file:" + generalizedPath else scheme + generalizedPath
  }

  /**
   * Normalize partitioned dataset paths to a logical dataset name
   *
   * Patterns detected:
   * 1. Hive-style partitioning: /data/date=2024-01-01/country=US/
   * 2. Date patterns: /data/2024-01-01/, /data/2024/01/01/
   * 3. Country codes: /data/US/, /data/UK/
   * 4. Common partition dirs: /year=X/, /month=X/, /day=X/, /country=X/, /region=X/
   *
   * Examples:
   * - "bucket/data/date=2024-01-01/country=US/" => "data"
   * - "bucket/data/2024-01-01/US/" => "data"
   * - "bucket/sales/year=2024/month=01/" => "sales"
   *
   * @param pathWithoutScheme Path without s3://, file://, etc.
   * @return Normalized dataset name
   */
  private def normalizePartitionedPath(pathWithoutScheme: String): String = {
    val parts = pathWithoutScheme.split("/").filter(_.nonEmpty)

    // Common partition patterns to strip
    val partitionPatterns = Seq(
      "^(date|year|month|day|hour|dt|partition_date)=.+".r,        // Hive-style date partitions
      "^(country|region|zone|geo|location|market)=.+".r,            // Hive-style geo partitions
      "^\\d{4}-\\d{2}-\\d{2}$".r,                                   // ISO date: 2024-01-01
      "^\\d{8}$".r,                                                 // Compact date: 20240101
      "^\\d{4}$".r,                                                 // Year: 2024
      "^(0[1-9]|1[0-2])$".r,                                        // Month: 01-12
      "^(0[1-9]|[12][0-9]|3[01])$".r,                               // Day: 01-31
      "^[A-Z]{2}$".r,                                               // 2-letter country code: US, UK
      "^[a-z]{2}-[a-z]+-\\d+$".r,                                   // AWS region: us-east-1
      "^(staging|prod|dev|test)$".r                                 // Environment
    )

    // Find the last non-partition part (going backwards from the end)
    val reversedParts = parts.reverse
    val baseDatasetIndex = reversedParts.indexWhere { part =>
      !partitionPatterns.exists(_.findFirstIn(part).isDefined)
    }

    if (baseDatasetIndex >= 0 && baseDatasetIndex < reversedParts.length) {
      val baseName = reversedParts(baseDatasetIndex)
      // Remove file extensions
      baseName.replaceAll("\\.(parquet|csv|json|orc|avro)$", "")
    } else {
      // Couldn't detect pattern, use last part
      parts.lastOption.getOrElse("unknown_dataset")
        .replaceAll("\\.(parquet|csv|json|orc|avro)$", "")
    }
  }

  /**
   * Helper: Extract just the table name from fully qualified name
   *
   * Examples:
   * - "spark_catalog.default.bronze_taxi_trips" => "bronze_taxi_trips"
   * - "bronze_taxi_trips" => "bronze_taxi_trips"
   *
   * @param fullName The full table identifier
   * @return Just the table name
   */
  private def extractTableName(fullName: String): String = {
    // Split by "." and take the last part
    // "spark_catalog.default.bronze_taxi_trips" => "bronze_taxi_trips"
    fullName.split("\\.").last
  }

  /**
   * Extract Datasource V2, JDBC, or Warehouse table information from relation string
   * Handles: PostgreSQL, MySQL, Redshift, Snowflake, BigQuery, etc.
   *
   * Example strings:
   * - "JDBCRelation(public.users) [url=jdbc:postgresql://host/db]"
   * - "SnowflakeRelation(table)"
   * - "BigQueryRelation(table)"
   */
  private def extractDatasourceTableFromString(relationString: String): Option[TableReference] = {
    // List of regex patterns to try
    val patterns = Seq(
      "JDBCRelation\\(([^)]+)\\)".r,                 // JDBC: JDBCRelation(schema.table)
      "SnowflakeRelation[^\\(]*\\(([^)]+)\\)".r,     // Snowflake: SnowflakeRelation(table)
      "GoogleBigQueryRelation[^\\(]*\\(([^)]+)\\)".r,// BigQuery
      "RedshiftRelation[^\\(]*\\(([^)]+)\\)".r,      // Redshift
      "dbtable=([^,\\]\\s]+)".r,                     // Generic dbtable parameter
      "CheckpointsV2\\s*\\[([^,\\]\\s]+)\\]".r       // Delta Lake checkpoints sometimes
    )

    patterns.foreach { pattern =>
      pattern.findFirstMatchIn(relationString) match {
        case Some(m) =>
          val tableName = m.group(1)
          logger.debug(s"Parsed datasource table: $tableName from ${pattern.toString}")
          return Some(TableReference(
            name = extractTableName(tableName),
            tableType = TableType.Table,
            location = Some(s"datasource:$tableName")
          ))
        case None => // Try next
      }
    }

    None
  }

  /**
   * Extract schema information from a logical plan node
   *
   * Logical plan nodes have an `output` field that contains the schema
   * (column names, types, nullability). We convert this to SchemaInfo.
   *
   * @param node The logical plan node
   * @return SchemaInfo if schema can be extracted, None otherwise
   */
  private def extractSchemaFromNode(node: LogicalPlan): Option[SchemaInfo] = {
    try {
      if (node.output.isEmpty) {
        return None
      }

      val fields = node.output.map { attr =>
        FieldInfo(
          name = attr.name,
          dataType = attr.dataType.simpleString,
          nullable = attr.nullable,
          metadata = Map.empty  // Could extract metadata from attr.metadata if needed
        )
      }

      val schemaInfo = SchemaInfo(fields, "")
      val hash = schemaInfo.computeHash()

      Some(schemaInfo.copy(schemaHash = hash))
    } catch {
      case e: Exception =>
        logger.trace(s"Could not extract schema from node: ${e.getMessage}")
        None
    }
  }

  /**
   * Extract schema from write command nodes
   *
   * Write commands (InsertIntoHadoopFsRelationCommand, CreateDataSourceTableAsSelectCommand, etc.)
   * have a 'query' field that contains the logical plan of the data being written.
   * We extract the schema from that query plan.
   *
   * This works for ALL output formats including Avro, Parquet, ORC, CSV, JSON, etc.
   *
   * @param commandNode The write command node
   * @return SchemaInfo if schema can be extracted, None otherwise
   */
  private def extractSchemaFromWriteCommand(commandNode: LogicalPlan): Option[SchemaInfo] = {
    try {
      // Try to access the 'query' field using reflection
      val queryMethod = commandNode.getClass.getMethod("query")
      val queryPlan = queryMethod.invoke(commandNode).asInstanceOf[LogicalPlan]

      // Extract schema from the query plan
      val schema = extractSchemaFromNode(queryPlan)

      if (schema.isDefined) {
        logger.debug(s"Extracted schema from write command: ${schema.get.fields.size} fields")
      } else {
        logger.debug(s"No schema found in write command query plan")
      }

      schema
    } catch {
      case e: NoSuchMethodException =>
        logger.trace(s"Write command does not have 'query' field: ${e.getMessage}")
        None
      case e: Exception =>
        logger.trace(s"Could not extract schema from write command: ${e.getMessage}")
        None
    }
  }
}