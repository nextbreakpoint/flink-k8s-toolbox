package com.nextbreakpoint.flink.integration.cases

import com.nextbreakpoint.flink.integration.IntegrationSetup
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterSpec
import io.kubernetes.client.openapi.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files

@Tag("IntegrationTest")
class CreateAndDeleteTest : IntegrationSetup() {
    companion object {
        @BeforeAll
        @JvmStatic
        fun setup() {
            IntegrationSetup.setup()
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            IntegrationSetup.teardown()
        }
    }

    @AfterEach
    fun printInfo() {
        printSupervisorLogs()
        printOperatorLogs()
        describeResources()
    }

    @Test
    fun `should create and delete clusters`() {
        println("Should create cluster...")
        val json = String(Files.readAllBytes(File("integration/cluster-spec.json").toPath()))
        val spec = JSON().deserialize<V2FlinkClusterSpec>(json, specTypeToken.type)
        createCluster(clusterName = "cluster-0", spec = spec, port = port)
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-0")).isTrue()
        }
        awaitUntilAsserted(timeout = 240) {
            assertThat(jobExists(namespace = namespace, jobName = "cluster-0-job-0")).isTrue()
        }
        describePods(namespace = namespace)
        describeJobs(namespace = namespace)
        describeClusters(namespace = namespace)
        println("Should delete cluster...")
        deleteCluster(clusterName = "cluster-0", port = port)
        awaitUntilAsserted(timeout = 120) {
            assertThat(jobExists(namespace = namespace, jobName = "cluster-0-job-0")).isFalse()
        }
        awaitUntilAsserted(timeout = 300) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-0")).isFalse()
        }
        describePods(namespace = namespace)
        describeJobs(namespace = namespace)
        describeClusters(namespace = namespace)
    }
}
