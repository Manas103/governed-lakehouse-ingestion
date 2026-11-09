package com.manas.lakehouse.generator

import com.manas.lakehouse.contract.FeedContract
import com.manas.lakehouse.contract.FieldContract
import com.manas.lakehouse.contract.FieldType
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/** One deliberately-planted bad value, by row index within the feed and canonical column name. */
data class SeededViolation(val rowIndex: Long, val canonicalColumn: String, val badValueSql: String)

/**
 * Builds a feed's raw synthetic DataFrame entirely in Spark SQL expressions over
 * `spark.range(n)`, so generating 1,000,000 rows is one distributed computation, not a
 * driver-side loop. Every value is a deterministic function of the row index and the
 * feed's name (so two feeds never coincidentally produce identical rows), with no real
 * account, position, or balance behind any of it.
 */
object SyntheticFeedGenerator {

    fun generate(
        spark: SparkSession,
        contract: FeedContract,
        rowCount: Long,
        seed: Long,
        seededViolations: List<SeededViolation> = emptyList(),
    ): Dataset<Row> {
        val salt = (contract.feedName.hashCode().toLong() xor seed) and 0x7fffffffL
        val violationsByColumn = seededViolations.groupBy { it.canonicalColumn }

        var df = spark.range(0, rowCount).toDF("idx")
        df = df.selectExpr("*", "idx as row_id_num")

        val rowIdExpr = "concat('${contract.feedName}-', cast(row_id_num as string))"
        val selectExprs = mutableListOf("row_id_num", "$rowIdExpr as `${contract.rowIdColumn}`")

        for (field in contract.suppliedFields()) {
            // Every raw column is generated as a string, the way a real CSV/JSON feed's
            // columns are untyped text before contract validation ever runs try_cast on
            // them; this also means a seeded bad value (an arbitrary string literal) is
            // always type-compatible with the column, instead of breaking Spark's CASE
            // WHEN/ELSE type unification at generation time, before validation gets a
            // chance to catch it as a contract violation.
            val normal = "cast(${normalValueExpr(field, salt)} as string)"
            val withOptionalNulls = if (!field.required) {
                "case when pmod(idx + $salt, 7) = 0 then NULL else $normal end"
            } else {
                normal
            }
            val seededForField = violationsByColumn[field.canonicalName]
            val finalExpr = if (seededForField.isNullOrEmpty()) {
                withOptionalNulls
            } else {
                val whenClauses = seededForField.joinToString(" ") { v ->
                    // Cast explicitly rather than leaving a bare literal: Spark's CASE WHEN
                    // type coercion picks a common type across every branch, and with a
                    // bare numeric literal in one branch and a string expression in
                    // another it can decide to coerce the *string* branch to that numeric
                    // type instead of the reverse, which throws at generation time on
                    // values like "7461.0" (not a valid integer literal) before the row
                    // ever reaches contract validation.
                    "when row_id_num = ${v.rowIndex} then cast(${v.badValueSql} as string)"
                }
                "case $whenClauses else $withOptionalNulls end"
            }
            selectExprs.add("$finalExpr as `${field.sourceColumn}`")
        }

        return df.selectExpr(*selectExprs.toTypedArray()).drop("row_id_num")
    }

    private fun normalValueExpr(field: FieldContract, salt: Long): String = when (field.type) {
        FieldType.DECIMAL -> {
            val min = field.minValue ?: 0.0
            val max = field.maxValue ?: (min + 100_000.0)
            val span = (max - min).coerceAtLeast(1.0)
            val bucket = (span.coerceAtMost(100_000.0)).toLong().coerceAtLeast(1L)
            "($min + pmod(idx * 7 + $salt, $bucket))"
        }
        FieldType.DATE -> "date_add(to_date('2025-01-01'), cast(pmod(idx + $salt, 650) as int))"
        FieldType.TIMESTAMP -> "to_timestamp(date_add(to_date('2025-01-01'), cast(pmod(idx + $salt, 650) as int)))"
        FieldType.STRING -> {
            val allowed = field.allowedValues
            if (allowed != null && allowed.isNotEmpty()) {
                val arr = allowed.joinToString(",") { "'$it'" }
                "element_at(array($arr), cast(pmod(idx + $salt, ${allowed.size}) as int) + 1)"
            } else {
                "concat('${field.canonicalName}-', cast(pmod(idx + $salt, 999983) as string))"
            }
        }
    }
}
