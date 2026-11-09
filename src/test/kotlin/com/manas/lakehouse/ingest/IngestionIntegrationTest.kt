package com.manas.lakehouse.ingest

import com.manas.lakehouse.feeds.FeedDefinitions
import com.manas.lakehouse.generator.SeededViolation
import com.manas.lakehouse.iceberg.IcebergSupport
import com.manas.lakehouse.lineage.LineageChecker
import com.manas.lakehouse.lineage.LineageManifest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.apache.spark.sql.SparkSession
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IngestionIntegrationTest {

    private lateinit var spark: SparkSession
    private lateinit var warehouseDir: java.io.File

    @BeforeAll
    fun setUp() {
        warehouseDir = java.nio.file.Files.createTempDirectory("lakehouse-it").toFile()
        spark = IcebergSupport.newSparkSession("ingestion-it", warehouseDir.absolutePath)
    }

    @AfterAll
    fun tearDown() {
        spark.stop()
        warehouseDir.deleteRecursively()
    }

    @Test
    fun catchesEverySeededViolationAndHarmonizesTheRest() {
        val rowCountPerFeed = FeedDefinitions.ALL.associate { it.feedName to 200L }
        val seeded = mapOf(
            FeedDefinitions.POSITIONS_FEED_A.feedName to listOf(SeededViolation(10, "quantity", "NULL")),
            FeedDefinitions.POSITIONS_FEED_B.feedName to listOf(SeededViolation(20, "price", "-5")),
            FeedDefinitions.CASH_FEED_A.feedName to listOf(SeededViolation(5, "balance", "'abc'")),
            FeedDefinitions.CASH_FEED_B.feedName to listOf(SeededViolation(7, "as_of_date", "'not-a-date'")),
            FeedDefinitions.NAV_FEED_A.feedName to listOf(SeededViolation(3, "nav_value", "NULL")),
            FeedDefinitions.NAV_FEED_B.feedName to listOf(SeededViolation(9, "shares_outstanding", "-100")),
        )

        val result = IngestionPipeline.run(spark, FeedDefinitions.ALL, rowCountPerFeed, seed = 42L, seededViolationsByFeed = seeded)

        assertEquals(6, result.quarantinedRowCount)
        assertEquals(6L * 200 - 6, result.admittedRowCount)

        val quarantineRows = result.quarantine.collectAsList()
        assertEquals(6, quarantineRows.size)
        for ((feedName, violations) in seeded) {
            for (v in violations) {
                val expectedRowId = "$feedName-${v.rowIndex}"
                val match = quarantineRows.find { it.getAs<String>("source_row_id") == expectedRowId && it.getAs<String>("column") == v.canonicalColumn }
                assertTrue(match != null, "expected a quarantine row for $expectedRowId/${v.canonicalColumn}")
                assertEquals(feedName, match!!.getAs<String>("feed_name"))
            }
        }

        assertEquals(setOf("positions", "cash", "nav"), result.harmonizedByTable.keys)
        // Each table is fed by 2 feeds of 200 rows, each with exactly 1 seeded violation.
        assertEquals(2L * 200 - 2, result.harmonizedByTable["positions"]!!.count())
        assertEquals(2L * 200 - 2, result.harmonizedByTable["cash"]!!.count())
        assertEquals(2L * 200 - 2, result.harmonizedByTable["nav"]!!.count())

        for ((table, feeds) in FeedDefinitions.ALL.groupBy { it.canonicalTable }) {
            val check = LineageChecker.verify(table, result.harmonizedByTable[table]!!, result.rawByFeed, feeds, sampleSize = 100)
            assertTrue(check.mismatches.isEmpty(), "lineage mismatches in $table: ${check.mismatches.take(5)}")
            assertTrue(check.valueComparisons > 0)
        }

        IcebergSupport.writeTable(spark, result.harmonizedByTable["positions"]!!, "positions")
        IcebergSupport.writeTable(spark, result.harmonizedByTable["cash"]!!, "cash")
        IcebergSupport.writeTable(spark, result.harmonizedByTable["nav"]!!, "nav")
        IcebergSupport.writeTable(spark, result.quarantine, "quarantine")

        assertEquals(2L * 200 - 2, spark.sql("select * from local.lakehouse.positions").count())
        assertEquals(6L, spark.sql("select * from local.lakehouse.quarantine").count())
    }

    @Test
    fun lineageManifestCoversExactlyNinetyFourColumns() {
        assertEquals(94, LineageManifest.ALL.size)
        assertEquals(32, LineageManifest.ALL.count { it.table == "positions" })
        assertEquals(30, LineageManifest.ALL.count { it.table == "cash" })
        assertEquals(32, LineageManifest.ALL.count { it.table == "nav" })
    }
}
