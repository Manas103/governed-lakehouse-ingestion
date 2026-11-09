package com.manas.lakehouse.ingest

import com.manas.lakehouse.contract.ContractValidator
import com.manas.lakehouse.contract.FeedContract
import com.manas.lakehouse.generator.SeededViolation
import com.manas.lakehouse.generator.SyntheticFeedGenerator
import com.manas.lakehouse.harmonize.Harmonizer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

data class IngestionResult(
    val harmonizedByTable: Map<String, Dataset<Row>>,
    val quarantine: Dataset<Row>,
    val rawByFeed: Map<String, Dataset<Row>>,
    val admittedRowCount: Long,
    val quarantinedRowCount: Long,
)

/**
 * Runs every feed through validate -> split -> harmonize, then unions each canonical
 * table's harmonized output across the feeds that write to it. This is the one place the
 * whole ingestion story (contract, quarantine, harmonization) comes together; everything
 * upstream of here is pure functions over a single feed.
 */
object IngestionPipeline {

    fun run(
        spark: SparkSession,
        feeds: List<FeedContract>,
        rowCountPerFeed: Map<String, Long>,
        seed: Long,
        seededViolationsByFeed: Map<String, List<SeededViolation>> = emptyMap(),
    ): IngestionResult {
        val rawByFeed = mutableMapOf<String, Dataset<Row>>()
        val harmonizedByFeed = mutableMapOf<String, Dataset<Row>>()
        val quarantineByFeed = mutableMapOf<String, Dataset<Row>>()
        var admittedCount = 0L
        var quarantinedCount = 0L

        for (feed in feeds) {
            val rowCount = rowCountPerFeed[feed.feedName] ?: error("no row count configured for ${feed.feedName}")
            val raw = SyntheticFeedGenerator.generate(
                spark, feed, rowCount, seed, seededViolationsByFeed[feed.feedName] ?: emptyList(),
            ).cache()
            rawByFeed[feed.feedName] = raw

            val annotated = ContractValidator.annotate(raw, feed)
            val admitted = ContractValidator.admitted(annotated)
            val quarantined = ContractValidator.quarantined(annotated, feed)
            val harmonized = Harmonizer.harmonize(admitted, feed)

            harmonizedByFeed[feed.feedName] = harmonized
            quarantineByFeed[feed.feedName] = quarantined
            admittedCount += admitted.count()
            quarantinedCount += quarantined.count()
        }

        val harmonizedByTable = feeds.groupBy { it.canonicalTable }.mapValues { (_, feedsForTable) ->
            feedsForTable.map { harmonizedByFeed[it.feedName]!! }.reduce { a, b -> a.unionByName(b) }
        }

        val quarantine = quarantineByFeed.values.reduce { a, b -> a.unionByName(b) }

        return IngestionResult(harmonizedByTable, quarantine, rawByFeed, admittedCount, quarantinedCount)
    }
}
