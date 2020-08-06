package com.nextbreakpoint.flink.integration.cases

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.integration.IntegrationSetup
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobStatus
import io.kubernetes.client.openapi.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.fail

@Tag("IntegrationTest")
class SavepointTest : IntegrationSetup() {
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
    fun `should create savepoint periodically`() {
        println("Should create clusters...")
        createCluster(namespace = namespace, path = "integration/cluster-2.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-2")).isTrue()
        }
        println("Should start clusters...")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-2", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updated)).isTrue()
        }
        println("Should start jobs...")
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-1", status = JobStatus.Started)).isTrue()
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-2", status = JobStatus.Started)).isTrue()
        }

        val response = getJobStatus(clusterName = "cluster-2", jobName = "job-1", port = port)
        println(response)
        assertThat(response["status"] as String?).isEqualTo("OK")
        val initialStatus = JSON().deserialize<V1FlinkJobStatus>(response["output"] as String, jobStatusTypeToken.type)
        assertThat(initialStatus.savepointMode).isEqualTo("Manual")
        assertThat(initialStatus.savepointPath).isBlank()
        assertThat(initialStatus.savepointTimestamp).isNull()

        println("Should trigger savepoint periodically...")
        if (updateCluster(namespace = namespace, clusterName = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/savepoint/savepointMode\",\"value\":\"Automatic\"}]") != 0) {
            fail("Can't update cluster")
        }
        if (updateJob(namespace = namespace, jobName = "cluster-2-job-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/savepoint/savepointMode\",\"value\":\"Automatic\"}]") != 0) {
            fail("Can't update job")
        }
        awaitUntilAsserted(timeout = 420) {
            val pollResponse = getJobStatus(clusterName = "cluster-2", jobName = "job-1", port = port)
            println(pollResponse)
            assertThat(pollResponse["status"] as String?).isEqualTo("OK")
            val latestStatus = JSON().deserialize<V1FlinkJobStatus>(pollResponse["output"] as String, jobStatusTypeToken.type)
            assertThat(latestStatus.savepointPath).isNotBlank()
            assertThat(latestStatus.savepointTimestamp).isNotNull()
        }

        val nextResponse = getJobStatus(clusterName = "cluster-2", jobName = "job-1", port = port)
        println(nextResponse)
        assertThat(nextResponse["status"] as String?).isEqualTo("OK")
        val currentStatus = JSON().deserialize<V1FlinkJobStatus>(nextResponse["output"] as String, jobStatusTypeToken.type)
        assertThat(currentStatus.savepointPath).isNotBlank()
        assertThat(currentStatus.savepointTimestamp).isNotNull()

        awaitUntilAsserted(timeout = 120) {
            val pollResponse = getJobStatus(clusterName = "cluster-2", jobName = "job-1", port = port)
            println(pollResponse)
            assertThat(pollResponse["status"] as String?).isEqualTo("OK")
            val latestStatus = JSON().deserialize<V1FlinkJobStatus>(pollResponse["output"] as String, jobStatusTypeToken.type)
            assertThat(latestStatus.savepointPath).isNotBlank()
            assertThat(latestStatus.savepointTimestamp).isNotNull()
            assertThat(latestStatus.savepointPath).isNotEqualTo(currentStatus.savepointPath)
            assertThat(latestStatus.savepointTimestamp).isGreaterThan(currentStatus.savepointTimestamp)
        }
    }

    @Test
    fun `should create savepoint manually`() {
        createCluster(namespace = namespace, path = "integration/cluster-1.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-1")).isTrue()
        }
        println("Should start clusters...")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-1", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-1", status = ResourceStatus.Updated)).isTrue()
        }
        println("Should start jobs...")
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
        }

        awaitUntilAsserted(timeout = 60) {
            val response = getJobStatus(clusterName = "cluster-1", jobName = "job-0", port = port)
            println(response)
            assertThat(response["status"] as String?).isEqualTo("OK")
            val initialStatus = JSON().deserialize<V1FlinkJobStatus>(response["output"] as String, jobStatusTypeToken.type)
            assertThat(initialStatus.savepointMode).isEqualTo("Manual")
            assertThat(initialStatus.savepointPath).isBlank()
            assertThat(initialStatus.savepointTimestamp).isNull()
        }

        println("Should trigger savepoint manually...")
        triggerSavepoint(clusterName = "cluster-1", jobName = "job-0", port = port)
        awaitUntilAsserted(timeout = 120) {
            val pollResponse = getJobStatus(clusterName = "cluster-1", jobName = "job-0", port = port)
            println(pollResponse)
            assertThat(pollResponse["status"] as String?).isEqualTo("OK")
            val latestStatus = JSON().deserialize<V1FlinkJobStatus>(pollResponse["output"] as String, jobStatusTypeToken.type)
            assertThat(latestStatus.savepointPath).isNotBlank()
            assertThat(latestStatus.savepointTimestamp).isNotNull()
        }
    }
}