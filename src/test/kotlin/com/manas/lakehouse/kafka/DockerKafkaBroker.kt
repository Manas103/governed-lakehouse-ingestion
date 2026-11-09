package com.manas.lakehouse.kafka

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import java.util.Properties

/**
 * Starts and stops a real, single-node Apache Kafka broker (KRaft, no Zookeeper, the
 * official `apache/kafka` image) in a Docker container this test starts and owns: the
 * container id is captured and only that id is ever stopped or removed, never a name or
 * PID discovered by listing what else is running. Port 9092 is the image's own baked-in
 * advertised listener; this only proceeds if that port is free, it is never forcibly
 * cleared.
 */
object DockerKafkaBroker {

    const val PORT = 9092
    private const val IMAGE = "apache/kafka:3.8.0"

    fun start(containerName: String): String {
        val proc = ProcessBuilder("docker", "run", "-d", "-p", "$PORT:9092", "--name", containerName, IMAGE)
            .redirectErrorStream(true)
            .start()
        val output = proc.inputStream.bufferedReader().readText().trim()
        val exitCode = proc.waitFor()
        check(exitCode == 0) { "docker run failed: $output" }
        val containerId = output.lines().last().trim()
        waitUntilReady(containerId)
        return containerId
    }

    private fun waitUntilReady(containerId: String, timeoutMs: Long = 60_000) {
        val deadline = System.currentTimeMillis() + timeoutMs
        var lastError: Exception? = null
        while (System.currentTimeMillis() < deadline) {
            try {
                val props = Properties()
                props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:$PORT"
                props[AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG] = "3000"
                AdminClient.create(props).use { it.listTopics().names().get() }
                return
            } catch (e: Exception) {
                lastError = e
                Thread.sleep(1500)
            }
        }
        throw IllegalStateException("Kafka broker in container $containerId never became ready", lastError)
    }

    fun stop(containerId: String) {
        ProcessBuilder("docker", "stop", containerId).start().waitFor()
        ProcessBuilder("docker", "rm", containerId).start().waitFor()
    }
}
