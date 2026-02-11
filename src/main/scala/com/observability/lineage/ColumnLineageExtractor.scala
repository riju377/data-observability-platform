package com.observability.lineage

import com.observability.models.{ColumnLineageEdge, TransformType}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

import scala.collection.mutable

/**
 * Extracts column-level lineage from Spark logical plans
 *
 * This analyzes how individual columns flow through transformations,
 * tracking the relationship between source and target columns.
 *
 * LEARNING GOALS:
 * - Understand Spark's expression tree structure
 * - Traverse expressions to find column references
 * - Map output columns back to their source columns
 */
object ColumnLineageExtractor extends LazyLogging {

  /**
   * Extract column-level lineage edges from a logical plan
   *
   * @param plan The Spark logical plan to analyze
   * @return Sequence of column lineage edges
   */
  def extractColumnLineage(plan: LogicalPlan): Seq[ColumnLineageEdge] = {
    logger.debug("Extracting column-level lineage")

    val edges = mutable.ListBuffer[ColumnLineageEdge]()

    // Find output table name (target)
    val outputTables = QueryPlanParser.extractOutputTables(plan)
    if (outputTables.isEmpty) {
      logger.debug("No output tables found, skipping column lineage extraction")
      return Seq.empty
    }

    val targetDataset = outputTables.head.name

    // Find input table names (sources)
    val inputTables = QueryPlanParser.extractInputTables(plan)
    val inputTableNames = inputTables.map(_.name).toSet

    logger.debug(s"Target dataset: $targetDataset")
    logger.debug(s"Source datasets: ${inputTableNames.mkString(", ")}")

    // Build a map of expression ID to source table/column
    val attributeOrigins = buildAttributeOrigins(plan)
    logger.trace(s"Attribute origins: ${attributeOrigins.size} mappings found")

    traversePlanForColumnLineage(plan, targetDataset, attributeOrigins, edges)

    val result = edges.toSeq.distinct
    logger.debug(s"Extracted ${result.size} column lineage edges")
    result
  }

  /**
   * Build a map from expression IDs to their source table and column
   *
   * This traverses the plan to find all leaf nodes (table scans) and
   * maps each column reference back to its source.
   */
  private def buildAttributeOrigins(plan: LogicalPlan): Map[Long, (String, String)] = {
    val origins = mutable.Map[Long, (String, String)]()

    def traverse(node: LogicalPlan, currentTable: Option[String]): Unit = {
      val className = node.getClass.getSimpleName

      // Skip command nodes (InsertIntoHadoopFsRelationCommand, CreateDataSourceTableAsSelectCommand, etc.)
      // These are wrapper nodes - the actual query with source tables is in their children
      if (className.contains("Command")) {
        logger.trace(s"Skipping command node: $className, traversing children directly")
        node.children.foreach(child => traverse(child, currentTable))
        return
      }

      // Determine table name for this node
      val tableName: Option[String] = className match {
        case "SubqueryAlias" =>
          try {
            val identifierField = node.getClass.getMethod("identifier")
            val identifier = identifierField.invoke(node)
            val nameField = identifier.getClass.getMethod("name")
            val fullName = nameField.invoke(identifier).asInstanceOf[String]
            Some(fullName.split("\\.").last)
          } catch {
            case _: Exception => currentTable
          }

        case "LogicalRelation" =>
          // Handle file-based reads (parquet, orc, etc.) without SubqueryAlias
          // Extract table name from file path or use input table names
          try {
            node match {
              case relation: org.apache.spark.sql.execution.datasources.LogicalRelation =>
                relation.catalogTable match {
                  case Some(table) => Some(table.identifier.table)
                  case None =>
                    // File-based read - extract from input tables
                    val inputTables = QueryPlanParser.extractInputTables(plan)
                    if (inputTables.nonEmpty) {
                      Some(inputTables.head.name)
                    } else {
                      currentTable
                    }
                }
              case _ => currentTable
            }
          } catch {
            case _: Exception => currentTable
          }

        case _ => currentTable
      }

      // Map output attributes to their source table
      if (tableName.isDefined) {
        node.output.foreach { attr =>
          origins.put(attr.exprId.id, (tableName.get, attr.name))
          logger.trace(s"Mapped attr ${attr.exprId.id} (${attr.name}) -> ${tableName.get}.${attr.name}")
        }
      }

      // Recurse into children
      node.children.foreach(child => traverse(child, tableName.orElse(currentTable)))
    }

    traverse(plan, None)
    origins.toMap
  }

  /**
   * Traverse the plan to find column lineage relationships
   */
  private def traversePlanForColumnLineage(
    plan: LogicalPlan,
    targetDataset: String,
    attributeOrigins: Map[Long, (String, String)],
    edges: mutable.ListBuffer[ColumnLineageEdge]
  ): Unit = {

    def traverse(node: LogicalPlan): Unit = {
      val className = node.getClass.getSimpleName

      className match {
        case "Project" =>
          // Project node contains the output columns
          try {
            val projectListField = node.getClass.getMethod("projectList")
            val projectList = projectListField.invoke(node).asInstanceOf[Seq[NamedExpression]]

            projectList.foreach { namedExpr =>
              val targetColumn = namedExpr.name
              val (sources, transformType, expression) = analyzeExpression(namedExpr, attributeOrigins)

              sources.foreach { case (sourceDataset, sourceColumn) =>
                edges += ColumnLineageEdge(
                  sourceDatasetName = sourceDataset,
                  sourceColumn = sourceColumn,
                  targetDatasetName = targetDataset,
                  targetColumn = targetColumn,
                  transformType = transformType,
                  expression = expression
                )
              }
            }
          } catch {
            case e: Exception =>
              logger.debug(s"Could not extract Project columns: ${e.getMessage}")
          }

        case name if name.contains("Aggregate") =>
          // Aggregate node contains grouping keys and aggregate expressions
          try {
            val aggregateExprsField = node.getClass.getMethod("aggregateExpressions")
            val aggregateExprs = aggregateExprsField.invoke(node).asInstanceOf[Seq[NamedExpression]]

            aggregateExprs.foreach { namedExpr =>
              val targetColumn = namedExpr.name
              val (sources, transformType, expression) = analyzeExpression(namedExpr, attributeOrigins)

              // Override transform type for aggregates
              val finalTransformType = if (isAggregateExpression(namedExpr)) {
                TransformType.AGGREGATE
              } else {
                transformType
              }

              sources.foreach { case (sourceDataset, sourceColumn) =>
                edges += ColumnLineageEdge(
                  sourceDatasetName = sourceDataset,
                  sourceColumn = sourceColumn,
                  targetDatasetName = targetDataset,
                  targetColumn = targetColumn,
                  transformType = finalTransformType,
                  expression = expression
                )
              }
            }
          } catch {
            case e: Exception =>
              logger.debug(s"Could not extract Aggregate columns: ${e.getMessage}")
          }

        case _ =>
          // Continue traversing
      }

      // Recurse into children
      node.children.foreach(traverse)
    }

    traverse(plan)
  }

  /**
   * Analyze an expression to find source columns and transformation type
   *
   * @return (sources: Seq[(dataset, column)], transformType, expressionString)
   */
  private def analyzeExpression(
    expr: Expression,
    attributeOrigins: Map[Long, (String, String)]
  ): (Seq[(String, String)], TransformType, Option[String]) = {

    val sources = mutable.ListBuffer[(String, String)]()
    var transformType: TransformType = TransformType.DIRECT
    var expressionStr: Option[String] = None

    def collectAttributes(e: Expression): Unit = {
      e match {
        case attr: AttributeReference =>
          attributeOrigins.get(attr.exprId.id) match {
            case Some((dataset, column)) =>
              sources += ((dataset, column))
            case None =>
              logger.trace(s"Unknown attribute origin for ${attr.name} (${attr.exprId.id})")
          }

        case alias: Alias =>
          // Analyze the child expression
          collectAttributes(alias.child)
          // Check if the child is more than a simple attribute reference
          if (!alias.child.isInstanceOf[AttributeReference]) {
            transformType = determineTransformType(alias.child)
            expressionStr = Some(formatExpression(alias.child))
          }

        case _ =>
          // For any other expression, determine type and collect all nested attributes
          if (!e.isInstanceOf[AttributeReference] && !e.isInstanceOf[Alias]) {
            transformType = determineTransformType(e)
            if (expressionStr.isEmpty) {
              expressionStr = Some(formatExpression(e))
            }
          }
          e.children.foreach(collectAttributes)
      }
    }

    collectAttributes(expr)
    (sources.toSeq.distinct, transformType, expressionStr)
  }

  /**
   * Determine the transformation type based on expression class
   */
  private def determineTransformType(expr: Expression): TransformType = {
    val className = expr.getClass.getSimpleName

    if (isAggregateExpression(expr)) {
      TransformType.AGGREGATE
    } else if (className.contains("Case") || className.contains("If")) {
      TransformType.CASE
    } else if (className.contains("And") || className.contains("Or") ||
               className.contains("Equal") || className.contains("Greater") ||
               className.contains("Less")) {
      TransformType.FILTER
    } else {
      TransformType.EXPRESSION
    }
  }

  /**
   * Check if expression is an aggregate function
   */
  private def isAggregateExpression(expr: Expression): Boolean = {
    val className = expr.getClass.getSimpleName
    val aggregateFunctions = Set(
      "Sum", "Count", "Avg", "Average", "Max", "Min",
      "First", "Last", "CollectList", "CollectSet",
      "StddevPop", "StddevSamp", "VariancePop", "VarianceSamp",
      "AggregateExpression"
    )

    aggregateFunctions.exists(className.contains) ||
      expr.children.exists(isAggregateExpression)
  }

  /**
   * Format an expression as a readable string
   */
  private def formatExpression(expr: Expression): String = {
    val className = expr.getClass.getSimpleName

    expr match {
      case attr: AttributeReference => attr.name
      case alias: Alias => formatExpression(alias.child)
      case _ =>
        // Try to get a clean representation
        val str = expr.sql
        if (str.length > 100) {
          str.take(97) + "..."
        } else {
          str
        }
    }
  }
}