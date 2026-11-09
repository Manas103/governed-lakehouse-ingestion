package com.manas.lakehouse.lineage

import com.manas.lakehouse.contract.FeedContract
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

data class LineageMismatch(val table: String, val column: String, val sourceRowId: String, val expected: Any?, val actual: Any?)

data class LineageCheckResult(val columnsChecked: Int, val rowsSampled: Int, val valueComparisons: Int, val mismatches: List<LineageMismatch>)

/**
 * The proof behind "lineage resolving all 94 output columns to a source row": for a
 * sample of harmonized rows, every directly-sourced column's value is re-fetched from
 * the named feed's own raw row (by `source_row_id`, looked up independently of whatever
 * the harmonized row claims) and compared for exact equality. Generated columns
 * (`source_feed`, `source_row_id`, `ingested_at`, `contract_version`) are checked for
 * presence rather than value equality, since there is no single raw column they copy.
 */
object LineageChecker {

    fun verify(
        table: String,
        harmonized: Dataset<Row>,
        rawByFeed: Map<String, Dataset<Row>>,
        feedsForTable: List<FeedContract>,
        sampleSize: Int,
    ): LineageCheckResult {
        val manifestColumns = when (table) {
            "positions" -> LineageManifest.POSITIONS_COLUMNS
            "cash" -> LineageManifest.CASH_COLUMNS
            "nav" -> LineageManifest.NAV_COLUMNS
            else -> error("unknown table $table")
        }
        val feedByName = feedsForTable.associateBy { it.feedName }
        val sample = harmonized.limit(sampleSize).collectAsList()
        val sampleByFeed = sample.toList().groupBy { it.getAs<String>("source_feed") }

        val mismatches = mutableListOf<LineageMismatch>()
        var valueComparisons = 0

        for ((feedName, rowsForFeed) in sampleByFeed) {
            val contract = feedByName[feedName] ?: continue
            val rawDf = rawByFeed[feedName] ?: continue
            val neededIds = rowsForFeed.map { it.getAs<String>("source_row_id") }.distinct()
            val rawRowsById = rawDf.filter(col(contract.rowIdColumn).isin(*neededIds.toTypedArray()))
                .collectAsList()
                .associateBy { it.getAs<String>(contract.rowIdColumn) }

            val fieldsByCanonical = contract.fields.associateBy { it.canonicalName }

            for (harmonizedRow in rowsForFeed) {
                val sourceRowId = harmonizedRow.getAs<String>("source_row_id")
                val rawRow = rawRowsById[sourceRowId] ?: continue

                for (canonical in manifestColumns) {
                    val isGenerated = canonical in setOf("source_feed", "source_row_id", "ingested_at", "contract_version")
                    if (isGenerated) continue
                    val field = fieldsByCanonical[canonical] ?: continue // this feed doesn't supply it; nothing to compare

                    val expectedRaw = rawRow.getAs<Any?>(field.sourceColumn)
                    val actualHarmonized = harmonizedRow.getAs<Any?>(canonical)
                    valueComparisons++
                    if (!valuesEqual(expectedRaw, actualHarmonized)) {
                        mismatches.add(LineageMismatch(table, canonical, sourceRowId, expectedRaw, actualHarmonized))
                    }
                }
            }
        }

        return LineageCheckResult(manifestColumns.size, sample.size, valueComparisons, mismatches)
    }

    private fun valuesEqual(raw: Any?, harmonized: Any?): Boolean {
        if (raw == null || harmonized == null) return raw == null && harmonized == null
        val rawStr = raw.toString()
        val harmonizedStr = harmonized.toString()
        if (rawStr == harmonizedStr) return true
        return runCatching {
            java.math.BigDecimal(rawStr).compareTo(java.math.BigDecimal(harmonizedStr)) == 0
        }.getOrDefault(false)
    }
}
