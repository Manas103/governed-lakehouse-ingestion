package com.manas.lakehouse.kafka

import com.manas.lakehouse.contract.ContractValidator
import com.manas.lakehouse.feeds.FeedDefinitions
import com.manas.lakehouse.generator.SeededViolation
import com.manas.lakehouse.generator.SyntheticFeedGenerator
import com.manas.lakehouse.harmonize.Harmonizer
import com.manas.lakehouse.iceberg.IcebergSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.ServerSocket
import java.time.Duration
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * One feed, `cash_bank_y`, delivered over a real Kafka topic instead of generated
 * straight into a Spark DataFrame: produced as JSON by a real `KafkaProducer` into a
 * real single-node broker (Docker, KRaft, started by this test, see
 * [DockerKafkaBroker]), consumed back by a real `KafkaConsumer`, and only then run
 * through the same [ContractValidator]/[Harmonizer] pipeline every other feed uses. The
 * 1,000,000-row volume benchmark does not use this path (disclosed in the README): it
 * generates all six feeds directly as Spark DataFrames for speed, because Kafka's own
 * throughput is not the thing under measurement there. This test is the genuine,
 * disclosed-as-smaller-scale proof that the Kafka half of the stack is real, not a stub.
 *
 * Skips cleanly (does not fail the build) if Docker is unreachable or port 9092 is
 * already in use by something this test did not start, consistent with this portfolio's
 * convention for infrastructure this environment does not run as a standing service.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EmbeddedKafkaFeedTest {

    private var containerId: String? = null
    private lateinit var spark: SparkSession
    private lateinit var warehouseDir: java.io.File

    @BeforeAll
    fun setUp() {
        Assumptions.assumeTrue(dockerAvailable(), "docker is not available in this environment")
        Assumptions.assumeTrue(portFree(DockerKafkaBroker.PORT), "port ${DockerKafkaBroker.PORT} is already in use")

        warehouseDir = java.nio.file.Files.createTempDirectory("lakehouse-kafka-it").toFile()
        spark = IcebergSupport.newSparkSession("kafka-feed-it", warehouseDir.absolutePath)
        containerId = DockerKafkaBroker.start("lakehouse-ingestion-test-kafka")
    }

    @AfterAll
    fun tearDown() {
        containerId?.let { DockerKafkaBroker.stop(it) }
        if (::spark.isInitialized) spark.stop()
        if (::warehouseDir.isInitialized) warehouseDir.deleteRecursively()
    }

    private fun dockerAvailable(): Boolean = runCatching {
        ProcessBuilder("docker", "version").start().waitFor() == 0
    }.getOrDefault(false)

    private fun portFree(port: Int): Boolean = runCatching {
        ServerSocket(port).close()
        true
    }.getOrDefault(false)

    @Test
    fun ingestsAFeedDeliveredOverARealKafkaTopic() {
        val feed = FeedDefinitions.CASH_FEED_B
        val rowCount = 300L
        val topic = "cash-bank-y-feed"
        val bootstrapServers = "localhost:${DockerKafkaBroker.PORT}"

        val produced = SyntheticFeedGenerator.generate(
            spark, feed, rowCount, seed = 777L,
            seededViolations = listOf(SeededViolation(42, "balance", "NULL")),
        )
        val jsonRows = produced.toJSON().collectAsList()
        assertEquals(rowCount.toInt(), jsonRows.size)

        val producerProps = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        }
        KafkaProducer<String, String>(producerProps).use { producer ->
            for (json in jsonRows) {
                producer.send(ProducerRecord(topic, null, json)).get()
            }
            producer.flush()
        }

        val consumerProps = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.GROUP_ID_CONFIG, "lakehouse-ingestion-test")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }
        val consumedJson = mutableListOf<String>()
        KafkaConsumer<String, String>(consumerProps).use { consumer ->
            consumer.subscribe(listOf(topic))
            val deadline = System.currentTimeMillis() + 30_000
            while (consumedJson.size < rowCount && System.currentTimeMillis() < deadline) {
                val records = consumer.poll(Duration.ofSeconds(2))
                records.forEach { consumedJson.add(it.value()) }
            }
        }
        assertEquals(rowCount.toInt(), consumedJson.size)

        val consumedDf = spark.read().json(spark.createDataset(consumedJson, Encoders.STRING()))
        val annotated = ContractValidator.annotate(consumedDf, feed)
        val admitted = ContractValidator.admitted(annotated)
        val quarantined = ContractValidator.quarantined(annotated, feed)

        assertEquals(1L, quarantined.count())
        assertEquals(rowCount - 1, admitted.count())
        val missedRow = quarantined.collectAsList().first()
        assertEquals("balance", missedRow.getAs<String>("column"))
        assertEquals("${feed.feedName}-42", missedRow.getAs<String>("source_row_id"))

        val harmonized = Harmonizer.harmonize(admitted, feed)
        IcebergSupport.writeTable(spark, harmonized, "cash")
        assertTrue(spark.sql("select * from local.lakehouse.cash").count() == rowCount - 1)
    }
}
