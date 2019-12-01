package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import io.kubernetes.client.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("IntegrationTest")
class ResourceStatusTest : IntegrationSetup() {
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
    fun `should return resource's status`() {
        println("Should return status...")
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
            assertThat(hasTaskStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = TaskStatus.Idle)).isTrue()
        }
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
            assertThat(hasTaskStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = TaskStatus.Idle)).isTrue()
        }
        val response1 = getClusterStatus(name = "cluster-1")
        println(response1)
        assertThat(response1["status"] as String?).isEqualTo("COMPLETED")
        val status1 = JSON().deserialize<V1FlinkClusterStatus>(response1["output"] as String, statusTypeToken.type)
        assertThat(status1.clusterStatus).isEqualTo(ClusterStatus.Running.toString())
        assertThat(status1.taskStatus).isEqualTo(TaskStatus.Idle.toString())
        assertThat(status1.savepointMode).isEqualTo("Manual")
        assertThat(status1.taskManagers).isEqualTo(1)
        assertThat(status1.activeTaskManagers).isEqualTo(1)
        assertThat(status1.serviceMode).isEqualTo("NodePort")
        assertThat(status1.jobParallelism).isEqualTo(1)
        assertThat(status1.taskSlots).isEqualTo(1)
        assertThat(status1.totalTaskSlots).isEqualTo(1)
        assertThat(status1.jobRestartPolicy).isEqualTo("Always")
        val response2 = getClusterStatus(name = "cluster-2")
        println(response2)
        assertThat(response2["status"] as String?).isEqualTo("COMPLETED")
        val status2 = JSON().deserialize<V1FlinkClusterStatus>(response2["output"] as String, statusTypeToken.type)
        assertThat(status2.clusterStatus).isEqualTo(ClusterStatus.Running.toString())
        assertThat(status2.taskStatus).isEqualTo(TaskStatus.Idle.toString())
        assertThat(status2.savepointMode).isEqualTo("Automatic")
        assertThat(status2.taskManagers).isEqualTo(2)
        assertThat(status2.activeTaskManagers).isEqualTo(2)
        assertThat(status2.serviceMode).isEqualTo("ClusterIP")
        assertThat(status2.jobParallelism).isEqualTo(4)
        assertThat(status2.taskSlots).isEqualTo(2)
        assertThat(status2.totalTaskSlots).isEqualTo(4)
        assertThat(status2.jobRestartPolicy).isEqualTo("Never")
    }
}