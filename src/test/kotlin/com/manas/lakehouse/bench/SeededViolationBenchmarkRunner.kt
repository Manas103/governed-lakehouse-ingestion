package com.manas.lakehouse.bench

import com.manas.lakehouse.contract.FeedContract
import com.manas.lakehouse.contract.FieldContract
import com.manas.lakehouse.contract.FieldType
import com.manas.lakehouse.feeds.FeedDefinitions
import com.manas.lakehouse.generator.SeededViolation
import com.manas.lakehouse.iceberg.IcebergSupport
import com.manas.lakehouse.ingest.IngestionPipeline
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import kotlin.test.assertEquals

/**
 * The 48-seeded-violation measurement behind the resume claim. Not part of `mvn test`
 * (tagged `benchmark`); run explicitly with
 * `mvn test -Dtest=SeededViolationBenchmarkRunner -Dsurefire.excludedGroups=`.
 *
 * 8 deliberately-planted bad values per feed (6 feeds x 8 = 48), each a required field
 * forced to either NULL or an out-of-contract value, at 2,000 rows/feed (12,000 rows
 * total) so the seeded violations are a small fraction of real volume, not the whole
 * feed. Every seeded violation must be quarantined naming its exact feed, row and column;
 * nothing else should be.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SeededViolationBenchmarkRunner {

    private lateinit var spark: SparkSession
    private lateinit var warehouseDir: java.io.File

    @BeforeAll
    fun setUp() {
        warehouseDir = Files.createTempDirectory("lakehouse-seeded-bench").toFile()
        spark = IcebergSupport.newSparkSession("seeded-violation-bench", warehouseDir.absolutePath, cores = 4)
    }

    @AfterAll
    fun tearDown() {
        spark.stop()
        warehouseDir.deleteRecursively()
    }

    private fun badValueFor(field: FieldContract, variant: Int): String = when (field.type) {
        FieldType.DECIMAL -> if (variant % 2 == 0) "NULL" else {
            val bad = (field.maxValue ?: 1_000_000.0) + 1_000_000.0
            bad.toString()
        }
        FieldType.DATE, FieldType.TIMESTAMP -> if (variant % 2 == 0) "NULL" else "'not-a-valid-date'"
        FieldType.STRING -> if (variant % 2 == 0 || field.allowedValues == null) "NULL" else "'NOT_A_VALID_ENUM_VALUE'"
    }

    private fun seededViolationsFor(feed: FeedContract, rowsPerFeed: Long, count: Int): List<SeededViolation> {
        val requiredFields = feed.fields.filter { it.required }
        require(requiredFields.isNotEmpty()) { "${feed.feedName} has no required fields to seed a violation on" }
        val step = rowsPerFeed / (count + 1)
        return (0 until count).map { i ->
            val field = requiredFields[i % requiredFields.size]
            SeededViolation(step * (i + 1), field.canonicalName, badValueFor(field, i))
        }
    }

    @Test
    @Tag("benchmark")
    fun catchesAllFortyEightSeededViolationsWithExactColumnAndRow() {
        val rowsPerFeed = 2000L
        val violationsPerFeed = 8
        val rowCountPerFeed = FeedDefinitions.ALL.associate { it.feedName to rowsPerFeed }
        val seeded = FeedDefinitions.ALL.associate { it.feedName to seededViolationsFor(it, rowsPerFeed, violationsPerFeed) }
        val totalSeeded = seeded.values.sumOf { it.size }
        assertEquals(48, totalSeeded)

        val result = IngestionPipeline.run(spark, FeedDefinitions.ALL, rowCountPerFeed, seed = 11011L, seededViolationsByFeed = seeded)
        val quarantineRows = result.quarantine.collectAsList()

        var caught = 0
        val misses = mutableListOf<String>()
        for ((feedName, violations) in seeded) {
            for (v in violations) {
                val expectedRowId = "$feedName-${v.rowIndex}"
                val found = quarantineRows.any {
                    it.getAs<String>("source_row_id") == expectedRowId && it.getAs<String>("column") == v.canonicalColumn
                }
                if (found) caught++ else misses.add("$feedName/$expectedRowId/${v.canonicalColumn}")
            }
        }

        val log = buildString {
            appendLine("Seeded violation benchmark: $caught / $totalSeeded seeded violations caught with exact feed/row/column")
            appendLine("Total quarantine rows: ${quarantineRows.size} (expect exactly $totalSeeded, no false positives)")
            if (misses.isNotEmpty()) {
                appendLine("Misses:")
                misses.forEach { appendLine("  $it") }
            }
        }
        Files.createDirectories(java.nio.file.Path.of("docs"))
        Files.writeString(java.nio.file.Path.of("docs/seeded_violation_benchmark_output.txt"), log)
        print(log)

        assertEquals(48, caught)
        assertEquals(48, quarantineRows.size)
    }
}
