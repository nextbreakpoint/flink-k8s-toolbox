package com.nextbreakpoint.flink.integration.cases

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.integration.IntegrationSetup
import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import io.kubernetes.client.openapi.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("IntegrationTest")
class DetailsAndMetricsTest : IntegrationSetup() {
    companion object {
        @BeforeAll
        @JvmStatic
        fun setup() {
            IntegrationSetup.setup()
            println("Should create clusters...")
            createResource(namespace = namespace, path = "integration/deployment-1.yaml")
            createResource(namespace = namespace, path = "integration/deployment-2.yaml")
            awaitUntilAsserted(timeout = 60) {
                assertThat(clusterExists(namespace = namespace, name = "cluster-1")).isTrue()
                assertThat(clusterExists(namespace = namespace, name = "cluster-2")).isTrue()
            }
            println("Should start clusters...")
            awaitUntilAsserted(timeout = 360) {
                assertThat(hasClusterStatus(namespace = namespace, name = "cluster-1", status = ClusterStatus.Started)).isTrue()
                assertThat(hasClusterStatus(namespace = namespace, name = "cluster-2", status = ClusterStatus.Started)).isTrue()
            }
            awaitUntilAsserted(timeout = 360) {
                assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-1", status = ResourceStatus.Updated)).isTrue()
                assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updated)).isTrue()
            }
            println("Should start jobs...")
            awaitUntilAsserted(timeout = 240) {
                assertThat(hasJobStatus(namespace = namespace, name = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
                assertThat(hasJobStatus(namespace = namespace, name = "cluster-2-job-1", status = JobStatus.Started)).isTrue()
                assertThat(hasJobStatus(namespace = namespace, name = "cluster-2-job-2", status = JobStatus.Started)).isTrue()
            }
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
    fun `should return job's details`() {
        println("Should return job details...")
        val response = getJobDetails(clusterName = "cluster-1", jobName = "job-0", port = port)
        assertThat(response["status"] as String?).isEqualTo("OK")
        val details = response["output"] as String
        assertThat(details).isNotBlank()
    }

    @Test
    fun `should return job's metrics`() {
        println("Should return job metrics...")
        val response = getJobMetrics(clusterName = "cluster-1", jobName = "job-0", port = port)
        assertThat(response["status"] as String?).isEqualTo("OK")
        val metrics = response["output"] as String
        assertThat(metrics).isNotBlank()
    }

    @Test
    fun `should return jobmanager's metrics`() {
        println("Should return JobManager details...")
        val response = getJobManagerMetrics(clusterName = "cluster-1", port = port)
        assertThat(response["status"] as String?).isEqualTo("OK")
        val metrics = response["output"] as String
        assertThat(metrics).isNotBlank()
    }

    @Test
    fun `should return taskmanagers`() {
        println("Should return TaskManagers...")
        val response1 = getTaskManagers(clusterName = "cluster-1", port = port)
        assertThat(response1["status"] as String?).isEqualTo("OK")
        val taskmanagers1 = JSON().deserialize<List<TaskManagerInfo>>(response1["output"] as String, taskmanagersTypeToken.type)
        assertThat(taskmanagers1).hasSize(1)
        val response2 = getTaskManagers(clusterName = "cluster-2", port = port)
        assertThat(response2["status"] as String?).isEqualTo("OK")
        val taskmanagers2 = JSON().deserialize<List<TaskManagerInfo>>(response2["output"] as String, taskmanagersTypeToken.type)
        assertThat(taskmanagers2).hasSize(2)
    }

    @Test
    fun `should return taskmanager's details`() {
        println("Should return TaskManager details...")
        val listResponse = getTaskManagers(clusterName = "cluster-1", port = port)
        assertThat(listResponse["status"] as String?).isEqualTo("OK")
        val taskmanagers = JSON().deserialize<List<TaskManagerInfo>>(listResponse["output"] as String, taskmanagersTypeToken.type)
        val detailsResponse = getTaskManagerDetails(clusterName = "cluster-1", taskmanagerId = TaskManagerId(taskmanagers[0].id), port = port)
        println(detailsResponse)
        assertThat(detailsResponse["status"] as String?).isEqualTo("OK")
        val details = detailsResponse["output"] as String
        assertThat(details).isNotBlank()
    }

    @Test
    fun `should return taskmanager's metrics`() {
        println("Should return TaskManager metrics...")
        val listResponse = getTaskManagers(clusterName = "cluster-2", port = port)
        assertThat(listResponse["status"] as String?).isEqualTo("OK")
        val taskmanagers = JSON().deserialize<List<TaskManagerInfo>>(listResponse["output"] as String, taskmanagersTypeToken.type)
        val metricsResponse1 = getTaskManagerMetrics(clusterName = "cluster-2", taskmanagerId = TaskManagerId(taskmanagers[0].id), port = port)
        println(metricsResponse1)
        assertThat(metricsResponse1["status"] as String?).isEqualTo("OK")
        val metrics1 = metricsResponse1["output"] as String
        assertThat(metrics1).isNotBlank()
        val metricsResponse2 = getTaskManagerMetrics(clusterName = "cluster-2", taskmanagerId = TaskManagerId(taskmanagers[1].id), port = port)
        println(metricsResponse2)
        assertThat(metricsResponse2["status"] as String?).isEqualTo("OK")
        val metrics2 = metricsResponse2["output"] as String
        assertThat(metrics2).isNotBlank()
    }
}