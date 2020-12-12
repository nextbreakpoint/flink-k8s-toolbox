package com.nextbreakpoint.flink.integration.cases

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.integration.IntegrationSetup
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
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
        printJobManagerLogs()
        printTaskManagerLogs()
        printBootstrapJobLogs()
        describeResources()
    }

    @Test
    fun `should create and delete clusters`() {
        println("Should create cluster...")
        val clusterSpecJson = String(Files.readAllBytes(File("integration/cluster-spec.json").toPath()))
        val clusterSpec = JSON().deserialize<V1FlinkClusterSpec>(clusterSpecJson, clusterSpecTypeToken.type)
        createCluster(clusterName = "cluster-0", spec = clusterSpec, port = port)
        awaitUntilAsserted(timeout = 60) {
            assertThat(clusterExists(namespace = namespace, name = "cluster-0")).isTrue()
        }
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasClusterStatus(namespace = namespace, name = "cluster-0", status = ClusterStatus.Starting)).isTrue()
        }
        describeClusters(namespace = namespace)
        println("Should delete cluster...")
        deleteCluster(clusterName = "cluster-0", port = port)
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasClusterStatus(namespace = namespace, name = "cluster-0", status = ClusterStatus.Stopping)).isTrue()
        }
        awaitUntilAsserted(timeout = 300) {
            assertThat(clusterExists(namespace = namespace, name = "cluster-0")).isFalse()
        }
        describeClusters(namespace = namespace)
    }

    @Test
    fun `should create and delete jobs`() {
        println("Should create job...")
        val clusterSpecJson = String(Files.readAllBytes(File("integration/cluster-spec.json").toPath()))
        val clusterSpec = JSON().deserialize<V1FlinkClusterSpec>(clusterSpecJson, clusterSpecTypeToken.type)
        val jobSpecJson = String(Files.readAllBytes(File("integration/job-spec.json").toPath()))
        val jobSpec = JSON().deserialize<V1FlinkJobSpec>(jobSpecJson, jobSpecTypeToken.type)
        createCluster(clusterName = "cluster-0", spec = clusterSpec, port = port)
        createJob(clusterName = "cluster-0", jobName = "job-0", spec = jobSpec, port = port)
        awaitUntilAsserted(timeout = 60) {
            assertThat(clusterExists(namespace = namespace, name = "cluster-0")).isTrue()
        }
        awaitUntilAsserted(timeout = 240) {
            assertThat(jobExists(namespace = namespace, name = "cluster-0-job-0")).isTrue()
        }
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasClusterStatus(namespace = namespace, name = "cluster-0", status = ClusterStatus.Starting)).isTrue()
        }
        describeJobs(namespace = namespace)
        println("Should delete job...")
        deleteJob(clusterName = "cluster-0", jobName = "job-0", port = port)
        deleteCluster(clusterName = "cluster-0", port = port)
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasClusterStatus(namespace = namespace, name = "cluster-0", status = ClusterStatus.Stopping)).isTrue()
        }
        awaitUntilAsserted(timeout = 120) {
            assertThat(jobExists(namespace = namespace, name = "cluster-0-job-0")).isFalse()
        }
        awaitUntilAsserted(timeout = 300) {
            assertThat(clusterExists(namespace = namespace, name = "cluster-0")).isFalse()
        }
        describeJobs(namespace = namespace)
    }
}
