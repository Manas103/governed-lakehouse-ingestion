package com.manas.lakehouse.bench

import com.manas.lakehouse.contract.ContractValidator
import com.manas.lakehouse.feeds.FeedDefinitions
import com.manas.lakehouse.generator.SeededViolation
import com.manas.lakehouse.generator.SyntheticFeedGenerator
import com.manas.lakehouse.harmonize.Harmonizer
import com.manas.lakehouse.iceberg.IcebergSupport
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.size
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import kotlin.system.measureTimeMillis
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The 1,000,000-row volume measurement behind the resume claim. Not part of `mvn test`
 * (tagged `benchmark`); run explicitly with
 * `mvn test -Dtest=VolumeBenchmarkRunner -Dsurefire.excludedGroups=`.
 *
 * 6 feeds summing to exactly 1,000,000 rows, with roughly 1 seeded violation per 2,000
 * rows per feed (500/feed, 3,000 total, a materially noisier rate than the 48-violation
 * suite's own measurement), and the claim checked is that every admitted row across all
 * 1,000,000 passed every contract check its feed declares; 0 contract-violating rows
 * reach the admitted/harmonized output, independent of how many violations the quarantine
 * path itself reports.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VolumeBenchmarkRunner {

    private lateinit var spark: SparkSession
    private lateinit var warehouseDir: java.io.File

    @BeforeAll
    fun setUp() {
        warehouseDir = Files.createTempDirectory("lakehouse-volume-bench").toFile()
        spark = IcebergSupport.newSparkSession("volume-bench", warehouseDir.absolutePath, cores = 4)
    }

    @AfterAll
    fun tearDown() {
        spark.stop()
        warehouseDir.deleteRecursively()
    }

    private fun seededNoiseFor(feedName: String, rowCount: Long, rate: Long = 2000): List<SeededViolation> {
        val feed = FeedDefinitions.ALL.first { it.feedName == feedName }
        val requiredFields = feed.fields.filter { it.required }
        val count = (rowCount / rate).toInt()
        return (0 until count).map { i ->
            val field = requiredFields[i % requiredFields.size]
            val rowIndex = i * rate + rate / 2
            SeededViolation(rowIndex, field.canonicalName, "NULL")
        }
    }

    @Test
    @Tag("benchmark")
    fun zeroContractViolatingRowsAdmittedOverOneMillionRecords() {
        val rowCountPerFeed = mapOf(
            FeedDefinitions.POSITIONS_FEED_A.feedName to 170_000L,
            FeedDefinitions.POSITIONS_FEED_B.feedName to 170_000L,
            FeedDefinitions.CASH_FEED_A.feedName to 165_000L,
            FeedDefinitions.CASH_FEED_B.feedName to 165_000L,
            FeedDefinitions.NAV_FEED_A.feedName to 165_000L,
            FeedDefinitions.NAV_FEED_B.feedName to 165_000L,
        )
        val totalRows = rowCountPerFeed.values.sum()
        assertEquals(1_000_000L, totalRows)

        val seeded = rowCountPerFeed.mapValues { (feedName, rows) -> seededNoiseFor(feedName, rows) }
        val totalSeeded = seeded.values.sumOf { it.size }

        var admittedAcrossAllFeeds = 0L
        var quarantinedAcrossAllFeeds = 0L
        var reAdmittedViolationCount = 0L
        val log = StringBuilder()

        val runMillis = measureTimeMillis {
            for (feed in FeedDefinitions.ALL) {
                val rows = rowCountPerFeed[feed.feedName]!!
                val raw = SyntheticFeedGenerator.generate(spark, feed, rows, seed = 20260101L, seededViolations = seeded[feed.feedName] ?: emptyList())
                val annotated = ContractValidator.annotate(raw, feed).cache()
                val admitted = ContractValidator.admitted(annotated)
                val quarantined = ContractValidator.quarantined(annotated, feed)

                val admittedCount = admitted.count()
                val quarantinedCount = quarantined.count()
                admittedAcrossAllFeeds += admittedCount
                quarantinedAcrossAllFeeds += quarantinedCount

                // Independent re-check, after harmonization: the same contract, rebuilt to
                // read the canonical (renamed, re-cast) columns instead of the raw ones, run
                // again against the harmonized output. This catches a bug harmonization
                // itself might introduce (a bad rename or a truncating cast) that the first
                // validation pass, run before harmonization, could never see.
                val harmonized = Harmonizer.harmonize(admitted, feed)
                val canonicalContract = feed.copy(
                    rowIdColumn = "source_row_id",
                    fields = feed.fields.map { it.copy(sourceColumn = it.canonicalName) },
                )
                val reViolations = ContractValidator.annotate(harmonized, canonicalContract)
                    .filter(size(col("violations")).gt(0))
                    .count()
                reAdmittedViolationCount += reViolations

                log.appendLine("${feed.feedName}: $rows rows, $admittedCount admitted, $quarantinedCount quarantined (seeded ${seeded[feed.feedName]?.size ?: 0}), $reViolations re-check violations after harmonization")
                annotated.unpersist()
            }
        }

        log.appendLine()
        log.appendLine("Total: $totalRows rows, $admittedAcrossAllFeeds admitted, $quarantinedAcrossAllFeeds quarantined, $totalSeeded seeded")
        log.appendLine("Run time: ${runMillis}ms")
        log.appendLine("Contract-violating rows admitted: $reAdmittedViolationCount (target 0)")

        Files.createDirectories(java.nio.file.Path.of("docs"))
        Files.writeString(java.nio.file.Path.of("docs/volume_benchmark_output.txt"), log.toString())
        print(log)

        assertEquals(totalRows, admittedAcrossAllFeeds + quarantinedAcrossAllFeeds)
        assertEquals(totalSeeded.toLong(), quarantinedAcrossAllFeeds)
        assertEquals(0L, reAdmittedViolationCount)
        assertTrue(admittedAcrossAllFeeds > 0)
    }
}
