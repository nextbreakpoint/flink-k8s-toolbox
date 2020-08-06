package com.nextbreakpoint.flink.integration.cases

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.integration.IntegrationSetup
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("IntegrationTest")
class BatchJobTest : IntegrationSetup() {
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
    fun `should stop job when job finished`() {
        println("Should create cluster...")
        createCluster(namespace = namespace, path = "integration/cluster-3.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-3")).isTrue()
        }
        println("Should start cluster...")
        awaitUntilAsserted(timeout = 420) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-3", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-3", status = ResourceStatus.Updated)).isTrue()
        }
        println("Should start batch job...")
        awaitUntilAsserted(timeout = 120) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-3-job-0", status = JobStatus.Started)).isTrue()
        }
        println("Should stop job when batch job has finished...")
        awaitUntilAsserted(timeout = 120) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-3-job-0", status = JobStatus.Stopped)).isTrue()
            assertThat(hasClusterJobStatus(namespace = namespace, jobName = "cluster-3-job-0", status = "FINISHED")).isTrue()
        }
        awaitUntilAsserted(timeout = 420) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-3", status = ClusterStatus.Started)).isTrue()
        }
    }
}