package com.manas.lakehouse.contract

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.size

/**
 * Pushes contract checking down into Spark SQL rather than collecting rows to the driver
 * or round-tripping through a Java bean encoder: for each field the feed actually
 * supplies, one `CASE WHEN NOT (<condition>) THEN '<column>'` expression is built, and
 * `array_remove(array(...), NULL)` collapses the per-field checks into one `violations`
 * array column per row. This is one dataframe operation over however many rows are in
 * the feed, not a row-at-a-time loop.
 */
object ContractValidator {

    fun annotate(raw: Dataset<Row>, contract: FeedContract): Dataset<Row> {
        val caseExprs = contract.suppliedFields().map { field ->
            val sourceCol = "`${field.sourceColumn}`"
            val (condition, reason) = buildCondition(sourceCol, field)
            val label = "${field.canonicalName}::${reason.replace("'", "")}"
            "CASE WHEN NOT ($condition) THEN '$label' END"
        }
        // Not array_remove(array(...), NULL): Spark's array_remove returns NULL for the
        // whole array when the *element to remove* argument is NULL, which is exactly our
        // case (every clean field contributes a NULL entry). filter() with an is-not-null
        // lambda is the function that actually drops the null entries and keeps the rest.
        val violationsExpr = if (caseExprs.isEmpty()) {
            "array()"
        } else {
            "filter(array(${caseExprs.joinToString(", ")}), x -> x is not null)"
        }
        return raw.selectExpr("*", "$violationsExpr as violations")
    }

    /** Returns (sqlCondition, humanReadableReasonIfItFails). */
    private fun buildCondition(col: String, field: FieldContract): Pair<String, String> {
        val (requiredCondition, reason) = buildRequiredCondition(col, field)
        if (!field.required) {
            return "($col is null or ($requiredCondition))" to reason
        }
        return "($col is not null and ($requiredCondition))" to "required but $reason"
    }

    private fun buildRequiredCondition(col: String, field: FieldContract): Pair<String, String> {
        val checks = mutableListOf("$col is not null")
        val reasons = mutableListOf("missing")
        when (field.type) {
            FieldType.DECIMAL -> {
                checks.add("try_cast($col as double) is not null")
                reasons.add("not numeric")
                field.minValue?.let {
                    checks.add("try_cast($col as double) >= $it")
                    reasons.add("below minimum $it")
                }
                field.maxValue?.let {
                    checks.add("try_cast($col as double) <= $it")
                    reasons.add("above maximum $it")
                }
            }
            FieldType.DATE -> {
                checks.add("try_cast($col as date) is not null")
                reasons.add("not a valid date")
            }
            FieldType.TIMESTAMP -> {
                checks.add("try_cast($col as timestamp) is not null")
                reasons.add("not a valid timestamp")
            }
            FieldType.STRING -> {
                field.allowedValues?.let { allowed ->
                    val list = allowed.joinToString(", ") { "'$it'" }
                    checks.add("$col in ($list)")
                    reasons.add("not one of $allowed")
                }
            }
        }
        return checks.joinToString(" and ") to reasons.joinToString(" / ")
    }

    /** Rows with zero violations, `violations` column dropped. */
    fun admitted(annotated: Dataset<Row>): Dataset<Row> =
        annotated.filter(size(col("violations")).equalTo(0)).drop("violations")

    /** One row per (feed, source row, column, reason) violation. */
    fun quarantined(annotated: Dataset<Row>, contract: FeedContract): Dataset<Row> =
        annotated.filter(size(col("violations")).gt(0))
            .select(
                org.apache.spark.sql.functions.lit(contract.feedName).alias("feed_name"),
                col(contract.rowIdColumn).cast("string").alias("source_row_id"),
                explode(col("violations")).alias("violation_label"),
            )
            .selectExpr(
                "feed_name",
                "source_row_id",
                "split(violation_label, '::')[0] as column",
                "split(violation_label, '::')[1] as reason",
            )
}
