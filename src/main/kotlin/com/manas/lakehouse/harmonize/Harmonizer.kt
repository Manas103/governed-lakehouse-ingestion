package com.manas.lakehouse.harmonize

import com.manas.lakehouse.contract.FeedContract
import com.manas.lakehouse.contract.FieldType
import com.manas.lakehouse.feeds.canonicalFieldType
import com.manas.lakehouse.lineage.LineageManifest
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

const val CONTRACT_VERSION = "v1"

/**
 * Renames and casts one feed's admitted (already contract-validated) rows into the
 * canonical column set for its table. A canonical column this feed does not supply is
 * cast NULL, explicitly, at its canonical type, so a later `unionByName` across feeds
 * never silently drops or mistypes a column.
 */
object Harmonizer {

    private fun sparkTypeName(type: FieldType): String = when (type) {
        FieldType.DECIMAL -> "decimal(20,4)"
        FieldType.DATE -> "date"
        FieldType.TIMESTAMP -> "timestamp"
        FieldType.STRING -> "string"
    }

    fun harmonize(admitted: Dataset<Row>, contract: FeedContract): Dataset<Row> {
        val canonicalCols = when (contract.canonicalTable) {
            "positions" -> LineageManifest.POSITIONS_COLUMNS
            "cash" -> LineageManifest.CASH_COLUMNS
            "nav" -> LineageManifest.NAV_COLUMNS
            else -> error("unknown canonical table ${contract.canonicalTable}")
        }
        val fieldsByCanonical = contract.fields.associateBy { it.canonicalName }

        val selectExprs = canonicalCols.map { canonical ->
            when (canonical) {
                "source_feed" -> "'${contract.feedName}' as source_feed"
                "source_row_id" -> "`${contract.rowIdColumn}` as source_row_id"
                "ingested_at" -> "current_timestamp() as ingested_at"
                "contract_version" -> "'$CONTRACT_VERSION' as contract_version"
                else -> {
                    val field = fieldsByCanonical[canonical]
                    val sqlType = sparkTypeName(canonicalFieldType(canonical))
                    if (field != null) {
                        "cast(`${field.sourceColumn}` as $sqlType) as $canonical"
                    } else {
                        "cast(NULL as $sqlType) as $canonical"
                    }
                }
            }
        }
        return admitted.selectExpr(*selectExprs.toTypedArray())
    }
}
