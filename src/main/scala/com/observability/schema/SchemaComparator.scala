package com.observability.schema

import com.observability.schema.SchemaTracker.{SchemaField, SchemaSnapshot}
import com.typesafe.scalalogging.LazyLogging

/**
 * Schema Comparison and Drift Detection
 *
 * LEARNING GOALS:
 * - Detect schema changes (additions, deletions, modifications)
 * - Classify changes by severity (breaking vs non-breaking)
 * - Generate human-readable change descriptions
 * - Understand schema evolution patterns
 *
 * WHAT IS SCHEMA DRIFT?
 * Schema drift occurs when a table's structure changes over time:
 * - New columns added
 * - Columns removed
 * - Data types changed
 * - Nullability changed
 *
 * WHY DOES IT MATTER?
 * - Breaking changes can crash downstream jobs
 * - Non-breaking changes might affect query performance
 * - Compliance: new PII columns need to be tracked
 * - Debugging: "Why did my job fail yesterday but not today?"
 */
object SchemaComparator extends LazyLogging {

  /**
   * Types of schema changes
   */
  sealed trait SchemaChange {
    def severity: ChangeSeverity
    def description: String
  }

  case class FieldAdded(field: SchemaField) extends SchemaChange {
    override val severity: ChangeSeverity = ChangeSeverity.NonBreaking
    override def description: String = s"Added field: ${field.name} (${field.dataType})"
  }

  case class FieldRemoved(field: SchemaField) extends SchemaChange {
    override val severity: ChangeSeverity = ChangeSeverity.Breaking
    override def description: String = s"Removed field: ${field.name}"
  }

  case class FieldTypeChanged(
    fieldName: String,
    oldType: String,
    newType: String
  ) extends SchemaChange {
    override val severity: ChangeSeverity = {
      if (isCompatibleTypeChange(oldType, newType)) ChangeSeverity.Warning
      else ChangeSeverity.Breaking
    }
    override def description: String = s"Type changed: $fieldName ($oldType → $newType)"
  }

  case class FieldNullabilityChanged(
    fieldName: String,
    wasNullable: Boolean,
    nowNullable: Boolean
  ) extends SchemaChange {
    override val severity: ChangeSeverity = {
      // Making a field nullable is generally safe (non-breaking)
      // Making a field non-nullable is risky (breaking) - could reject existing data
      if (nowNullable) ChangeSeverity.NonBreaking
      else ChangeSeverity.Breaking
    }
    override def description: String = {
      val from = if (wasNullable) "nullable" else "not null"
      val to = if (nowNullable) "nullable" else "not null"
      s"Nullability changed: $fieldName ($from → $to)"
    }
  }

  case class FieldReordered(fieldName: String, oldPosition: Int, newPosition: Int) extends SchemaChange {
    override val severity: ChangeSeverity = ChangeSeverity.Warning
    override def description: String = s"Field reordered: $fieldName (position $oldPosition → $newPosition)"
  }

  /**
   * Severity levels for schema changes
   *
   * BREAKING: Will likely break existing queries/jobs
   *   - Removed columns
   *   - Type changes (incompatible)
   *   - Nullable → Non-nullable
   *
   * WARNING: Might affect queries but won't break them
   *   - Type changes (compatible, e.g., int → bigint)
   *   - Column reordering
   *
   * NON_BREAKING: Safe changes
   *   - Added columns
   *   - Non-nullable → Nullable
   */
  sealed trait ChangeSeverity {
    def level: Int
    def symbol: String
  }

  object ChangeSeverity {
    case object Breaking extends ChangeSeverity {
      val level = 3
      val symbol = "❌"
    }
    case object Warning extends ChangeSeverity {
      val level = 2
      val symbol = "⚠️"
    }
    case object NonBreaking extends ChangeSeverity {
      val level = 1
      val symbol = "✅"
    }
  }

  /**
   * Result of schema comparison
   */
  case class SchemaComparisonResult(
    oldSchema: SchemaSnapshot,
    newSchema: SchemaSnapshot,
    changes: Seq[SchemaChange],
    hasBreakingChanges: Boolean,
    hasWarnings: Boolean,
    isIdentical: Boolean
  ) {
    def overallSeverity: ChangeSeverity = {
      if (hasBreakingChanges) ChangeSeverity.Breaking
      else if (hasWarnings) ChangeSeverity.Warning
      else ChangeSeverity.NonBreaking
    }

    def formatSummary: String = {
      if (isIdentical) {
        return "✅ Schemas are identical (no changes)"
      }

      val sb = new StringBuilder()
      sb.append(s"${overallSeverity.symbol} Schema Comparison: ${changes.size} change(s) detected\n")
      sb.append(s"  Old schema hash: ${oldSchema.schemaHash.take(16)}...\n")
      sb.append(s"  New schema hash: ${newSchema.schemaHash.take(16)}...\n")
      sb.append("\n")

      // Group by severity
      val breaking = changes.filter(_.severity == ChangeSeverity.Breaking)
      val warnings = changes.filter(_.severity == ChangeSeverity.Warning)
      val nonBreaking = changes.filter(_.severity == ChangeSeverity.NonBreaking)

      if (breaking.nonEmpty) {
        sb.append(s"❌ BREAKING CHANGES (${breaking.size}):\n")
        breaking.foreach(c => sb.append(s"   - ${c.description}\n"))
        sb.append("\n")
      }

      if (warnings.nonEmpty) {
        sb.append(s"⚠️  WARNINGS (${warnings.size}):\n")
        warnings.foreach(c => sb.append(s"   - ${c.description}\n"))
        sb.append("\n")
      }

      if (nonBreaking.nonEmpty) {
        sb.append(s"✅ NON-BREAKING CHANGES (${nonBreaking.size}):\n")
        nonBreaking.foreach(c => sb.append(s"   - ${c.description}\n"))
      }

      sb.toString()
    }
  }

  /**
   * Compare two schema snapshots
   *
   * ALGORITHM:
   * 1. Quick check: Are hashes identical? → No changes
   * 2. Build field maps by name for fast lookup
   * 3. Find added fields (in new, not in old)
   * 4. Find removed fields (in old, not in new)
   * 5. Find modified fields (in both, but different)
   * 6. Check for reordering
   *
   * @param oldSchema Previous schema version
   * @param newSchema Current schema version
   * @return Detailed comparison result
   */
  def compareSchemas(
    oldSchema: SchemaSnapshot,
    newSchema: SchemaSnapshot
  ): SchemaComparisonResult = {

    logger.debug(s"Comparing schemas: ${oldSchema.schemaHash} vs ${newSchema.schemaHash}")

    // Fast path: identical schemas
    if (oldSchema.schemaHash == newSchema.schemaHash) {
      logger.debug("  Schemas are identical (same hash)")
      return SchemaComparisonResult(
        oldSchema = oldSchema,
        newSchema = newSchema,
        changes = Seq.empty,
        hasBreakingChanges = false,
        hasWarnings = false,
        isIdentical = true
      )
    }

    val changes = scala.collection.mutable.ArrayBuffer[SchemaChange]()

    // Build maps for fast lookup
    val oldFields = oldSchema.fields.map(f => f.name -> f).toMap
    val newFields = newSchema.fields.map(f => f.name -> f).toMap

    // Find added fields
    val addedFieldNames = newFields.keySet -- oldFields.keySet
    addedFieldNames.foreach { name =>
      changes += FieldAdded(newFields(name))
    }

    // Find removed fields
    val removedFieldNames = oldFields.keySet -- newFields.keySet
    removedFieldNames.foreach { name =>
      changes += FieldRemoved(oldFields(name))
    }

    // Find modified fields (present in both)
    val commonFieldNames = oldFields.keySet.intersect(newFields.keySet)
    commonFieldNames.foreach { name =>
      val oldField = oldFields(name)
      val newField = newFields(name)

      // Check type change
      if (oldField.dataType != newField.dataType) {
        changes += FieldTypeChanged(name, oldField.dataType, newField.dataType)
      }

      // Check nullability change
      if (oldField.nullable != newField.nullable) {
        changes += FieldNullabilityChanged(name, oldField.nullable, newField.nullable)
      }
    }

    // Check for reordering (only for fields that exist in both)
    val oldPositions = oldSchema.fields.zipWithIndex.filter { case (f, _) =>
      commonFieldNames.contains(f.name)
    }.map { case (f, i) => f.name -> i }.toMap

    val newPositions = newSchema.fields.zipWithIndex.filter { case (f, _) =>
      commonFieldNames.contains(f.name)
    }.map { case (f, i) => f.name -> i }.toMap

    commonFieldNames.foreach { name =>
      val oldPos = oldPositions(name)
      val newPos = newPositions(name)
      if (oldPos != newPos) {
        changes += FieldReordered(name, oldPos, newPos)
      }
    }

    // Classify severity
    val hasBreaking = changes.exists(_.severity == ChangeSeverity.Breaking)
    val hasWarnings = changes.exists(_.severity == ChangeSeverity.Warning)

    val result = SchemaComparisonResult(
      oldSchema = oldSchema,
      newSchema = newSchema,
      changes = changes.toSeq.sortBy(_.severity.level).reverse,  // Breaking first
      hasBreakingChanges = hasBreaking,
      hasWarnings = hasWarnings,
      isIdentical = false
    )

    logger.debug(s"  Found ${changes.size} changes (breaking: $hasBreaking, warnings: $hasWarnings)")

    result
  }

  /**
   * Check if a type change is compatible
   *
   * COMPATIBLE TYPE CHANGES (safe upcasts):
   * - int → bigint (widening)
   * - float → double (widening)
   * - string → varchar (usually safe)
   *
   * INCOMPATIBLE TYPE CHANGES:
   * - bigint → int (narrowing, data loss)
   * - string → int (parsing required)
   * - struct → map (different semantics)
   */
  private def isCompatibleTypeChange(oldType: String, newType: String): Boolean = {
    (oldType, newType) match {
      // Numeric widening
      case ("int", "bigint") => true
      case ("float", "double") => true
      case ("tinyint", "int") => true
      case ("tinyint", "bigint") => true
      case ("smallint", "int") => true
      case ("smallint", "bigint") => true

      // String variations
      case ("string", s) if s.startsWith("varchar") => true
      case (v, "string") if v.startsWith("varchar") => true

      // Everything else is potentially breaking
      case _ => false
    }
  }

  /**
   * Quick check: Have schemas changed?
   *
   * Just compares hashes - fast but no details
   */
  def hasSchemaChanged(oldHash: String, newHash: String): Boolean = {
    oldHash != newHash
  }

  /**
   * Format changes for logging
   */
  def formatChangesForLog(changes: Seq[SchemaChange]): String = {
    if (changes.isEmpty) {
      "No schema changes"
    } else {
      changes.map(c => s"${c.severity.symbol} ${c.description}").mkString("\n")
    }
  }
}
