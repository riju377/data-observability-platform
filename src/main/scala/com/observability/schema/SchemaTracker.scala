package com.observability.schema

import org.apache.spark.sql.types._
import com.typesafe.scalalogging.LazyLogging

import java.security.MessageDigest

/**
 * Schema Tracking and Versioning
 *
 * LEARNING GOALS:
 * - Spark schema introspection
 * - Schema hashing for quick comparison
 * - JSON serialization of schemas
 * - Metadata extraction from StructType
 *
 * WHAT IS A SCHEMA?
 * In Spark, a schema is the structure of a DataFrame/Dataset:
 * - Column names
 * - Data types (Int, String, Struct, Array, etc.)
 * - Nullability (can it be null?)
 * - Metadata (comments, constraints, etc.)
 *
 * WHY TRACK SCHEMAS?
 * 1. Detect breaking changes (column removed, type changed)
 * 2. Track evolution over time (when was column X added?)
 * 3. Debug production issues ("schema mismatch")
 * 4. Compliance (know what PII fields exist)
 */
object SchemaTracker extends LazyLogging {

  /**
   * Simplified schema representation for storage
   *
   * We convert Spark's StructType to this simpler format because:
   * - StructType contains complex nested objects
   * - We need a flat representation for database storage
   * - Easier to compare and version
   */
  case class SchemaField(
    name: String,
    dataType: String,
    nullable: Boolean,
    comment: Option[String] = None
  )

  case class SchemaSnapshot(
    fields: Seq[SchemaField],
    schemaHash: String,
    fieldCount: Int
  )

  /**
   * Extract schema from Spark StructType
   *
   * Converts Spark's complex schema representation to our simpler format
   *
   * @param schema Spark StructType (DataFrame.schema)
   * @return SchemaSnapshot with fields and hash
   */
  def captureSchema(schema: StructType): SchemaSnapshot = {
    logger.debug(s"Capturing schema with ${schema.fields.length} fields")

    val fields = schema.fields.map { field =>
      SchemaField(
        name = field.name,
        dataType = field.dataType.simpleString,  // e.g., "string", "int", "struct<x:int,y:string>"
        nullable = field.nullable,
        comment = field.getComment()  // Extract comment if present
      )
    }.toSeq

    val hash = calculateSchemaHash(schema)

    val snapshot = SchemaSnapshot(
      fields = fields,
      schemaHash = hash,
      fieldCount = fields.length
    )

    logger.debug(s"  Schema hash: $hash")
    logger.debug(s"  Field count: ${fields.length}")

    snapshot
  }

  /**
   * Calculate hash of schema structure
   *
   * WHY HASH?
   * - Quick equality check: "Has schema changed?"
   * - Without hash, we'd need to compare every field
   * - Hash is like a fingerprint of the schema
   *
   * WHAT'S INCLUDED IN HASH?
   * - Column names (order matters!)
   * - Data types
   * - Nullability
   * - NOT included: comments, metadata (those can change without breaking compatibility)
   *
   * ALGORITHM: SHA-256
   * - Industry standard cryptographic hash
   * - Collision-resistant (two different schemas won't have same hash)
   * - Fast to compute
   *
   * @param schema Spark StructType
   * @return 64-character hex string (SHA-256)
   */
  def calculateSchemaHash(schema: StructType): String = {
    // Create canonical string representation
    // Format: "name:type:nullable|name:type:nullable|..."
    val schemaString = schema.fields.map { field =>
      s"${field.name}:${field.dataType.simpleString}:${field.nullable}"
    }.mkString("|")

    logger.trace(s"Schema string for hashing: $schemaString")

    // Calculate SHA-256 hash
    val digest = MessageDigest.getInstance("SHA-256")
    val hashBytes = digest.digest(schemaString.getBytes("UTF-8"))

    // Convert to hex string
    hashBytes.map("%02x".format(_)).mkString
  }

  /**
   * Convert schema to JSON string for storage
   *
   * We store schemas as JSON in PostgreSQL JSONB column because:
   * - Flexible: can add new fields without schema migration
   * - Queryable: PostgreSQL can query inside JSONB
   * - Human-readable: easy to inspect in database
   *
   * FORMAT:
   * {
   *   "fields": [
   *     {"name": "id", "type": "int", "nullable": false},
   *     {"name": "name", "type": "string", "nullable": true}
   *   ]
   * }
   */
  def schemaToJson(snapshot: SchemaSnapshot): String = {
    val fieldsJson = snapshot.fields.map { field =>
      val commentJson = field.comment.map(c => s""""comment": "$c",""").getOrElse("")
      s"""{
         |    "name": "${field.name}",
         |    "type": "${field.dataType}",
         |    "nullable": ${field.nullable}${if (commentJson.nonEmpty) "," else ""}
         |    $commentJson
         |  }""".stripMargin.replaceAll("\n\\s*\n", "\n")  // Remove empty lines
    }.mkString(",\n  ")

    s"""{
       |  "fields": [
       |  $fieldsJson
       |  ],
       |  "fieldCount": ${snapshot.fieldCount}
       |}""".stripMargin
  }

  /**
   * Get human-readable schema summary
   *
   * Example output:
   * Schema (5 fields):
   *   - id: int (not null)
   *   - name: string (nullable)
   *   - created_at: timestamp (not null)
   */
  def formatSchemaForDisplay(snapshot: SchemaSnapshot): String = {
    val sb = new StringBuilder()
    sb.append(s"Schema (${snapshot.fieldCount} fields):\n")

    snapshot.fields.foreach { field =>
      val nullStr = if (field.nullable) "nullable" else "not null"
      val commentStr = field.comment.map(c => s" -- $c").getOrElse("")
      sb.append(f"  - ${field.name}%-30s ${field.dataType}%-20s ($nullStr)$commentStr\n")
    }

    sb.append(s"Hash: ${snapshot.schemaHash.take(16)}...\n")

    sb.toString()
  }

  /**
   * Extract schema from Spark logical plan node
   *
   * This is useful when we want to capture schema from query plans
   * instead of DataFrames
   *
   * @param planNode Logical plan node
   * @return Option[SchemaSnapshot] if node has schema
   */
  def extractSchemaFromPlan(planNode: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Option[SchemaSnapshot] = {
    try {
      // LogicalPlan has an output attribute that contains schema info
      val schema = StructType(planNode.output.map { attr =>
        StructField(
          name = attr.name,
          dataType = attr.dataType,
          nullable = attr.nullable,
          metadata = attr.metadata
        )
      })

      Some(captureSchema(schema))
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to extract schema from plan node: ${e.getMessage}")
        None
    }
  }

  /**
   * Check if schema is empty (no fields)
   */
  def isEmpty(snapshot: SchemaSnapshot): Boolean = {
    snapshot.fieldCount == 0
  }

  /**
   * Get schema field names only (for quick lookup)
   */
  def getFieldNames(snapshot: SchemaSnapshot): Seq[String] = {
    snapshot.fields.map(_.name)
  }

  /**
   * Check if schema contains a specific field
   */
  def hasField(snapshot: SchemaSnapshot, fieldName: String): Boolean = {
    snapshot.fields.exists(_.name == fieldName)
  }

  /**
   * Get schema field by name
   */
  def getField(snapshot: SchemaSnapshot, fieldName: String): Option[SchemaField] = {
    snapshot.fields.find(_.name == fieldName)
  }
}
