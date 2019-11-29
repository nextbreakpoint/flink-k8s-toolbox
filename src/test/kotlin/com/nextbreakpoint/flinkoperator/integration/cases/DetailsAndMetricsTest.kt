package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import io.kubernetes.client.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("IntegrationTest")
class DetailsAndMetricsTest : IntegrationSetup() {
    companion object {
        @BeforeAll
        @JvmStatic
        fun createClusters() {
            println("Creating clusters...")
            createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-1.yaml")
            createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-2.yaml")
            awaitUntilAsserted(timeout = 60) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isTrue()
            }
            awaitUntilAsserted(timeout = 60) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-2")).isTrue()
            }
            println("Clusters created")
            println("Waiting for clusters...")
            awaitUntilAsserted(timeout = 180) {
                assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
            }
            awaitUntilAsserted(timeout = 180) {
                assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
            }
            println("Clusters started")
        }

        @AfterAll
        @JvmStatic
        fun deleteClusters() {
            println("Deleting clusters...")
            deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-1.yaml")
            deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-2.yaml")
            awaitUntilAsserted(timeout = 60) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isFalse()
            }
            awaitUntilAsserted(timeout = 60) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-2")).isFalse()
            }
            println("Clusters deleted")
        }
    }

    @Test
    fun `should return job's details`() {
        println("Should return job details...")
        val response = getJobDetails(name = "cluster-1")
        println(response)
        assertThat(response["status"] as String?).isEqualTo("COMPLETED")
        val details = response["output"] as String
        assertThat(details).isNotBlank()
    }

    @Test
    fun `should return job's metrics`() {
        println("Should return job metrics...")
        val response = getJobMetrics(name = "cluster-1")
        println(response)
        assertThat(response["status"] as String?).isEqualTo("COMPLETED")
        val metrics = response["output"] as String
        assertThat(metrics).isNotBlank()
    }

    @Test
    fun `should return jobmanager's metrics`() {
        println("Should return JobManager details...")
        val response = getJobManagerMetrics(name = "cluster-1")
        println(response)
        assertThat(response["status"] as String?).isEqualTo("COMPLETED")
        val metrics = response["output"] as String
        assertThat(metrics).isNotBlank()
    }

    @Test
    fun `should return taskmanagers`() {
        println("Should return TaskManagers...")
        val response1 = getTaskManagers(name = "cluster-1")
        assertThat(response1["status"] as String?).isEqualTo("COMPLETED")
        val taskmanagers1 = JSON().deserialize<List<TaskManagerInfo>>(response1["output"] as String, taskmanagersTypeToken.type)
        assertThat(taskmanagers1).hasSize(1)
        val response2 = getTaskManagers(name = "cluster-2")
        assertThat(response2["status"] as String?).isEqualTo("COMPLETED")
        val taskmanagers2 = JSON().deserialize<List<TaskManagerInfo>>(response2["output"] as String, taskmanagersTypeToken.type)
        assertThat(taskmanagers2).hasSize(2)
    }

    @Test
    fun `should return taskmanager's details`() {
        println("Should return TaskManager details...")
        val listResponse = getTaskManagers(name = "cluster-1")
        assertThat(listResponse["status"] as String?).isEqualTo("COMPLETED")
        val taskmanagers = JSON().deserialize<List<TaskManagerInfo>>(listResponse["output"] as String, taskmanagersTypeToken.type)
        val detailsResponse = getTaskManagerDetails(name = "cluster-1", taskmanagerId = TaskManagerId(taskmanagers[0].id))
        println(detailsResponse)
        assertThat(detailsResponse["status"] as String?).isEqualTo("COMPLETED")
        val details = detailsResponse["output"] as String
        assertThat(details).isNotBlank()
    }

    @Test
    fun `should return taskmanager's metrics`() {
        println("Should return TaskManager metrics...")
        val listResponse = getTaskManagers(name = "cluster-2")
        assertThat(listResponse["status"] as String?).isEqualTo("COMPLETED")
        val taskmanagers = JSON().deserialize<List<TaskManagerInfo>>(listResponse["output"] as String, taskmanagersTypeToken.type)
        val metricsResponse1 = getTaskManagerMetrics(name = "cluster-2", taskmanagerId = TaskManagerId(taskmanagers[0].id))
        println(metricsResponse1)
        assertThat(metricsResponse1["status"] as String?).isEqualTo("COMPLETED")
        val metrics1 = metricsResponse1["output"] as String
        assertThat(metrics1).isNotBlank()
        val metricsResponse2 = getTaskManagerMetrics(name = "cluster-2", taskmanagerId = TaskManagerId(taskmanagers[1].id))
        println(metricsResponse2)
        assertThat(metricsResponse2["status"] as String?).isEqualTo("COMPLETED")
        val metrics2 = metricsResponse2["output"] as String
        assertThat(metrics2).isNotBlank()
    }
}