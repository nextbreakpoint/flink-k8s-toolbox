package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import io.kubernetes.client.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("IntegrationTest")
class ScaleUpAndDownTest : IntegrationSetup() {
    companion object {
        @BeforeAll
        @JvmStatic
        fun createClusters() {
            println("Creating clusters...")
            createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-1.yaml")
            createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-2.yaml")
            awaitUntilAsserted(timeout = 30) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isTrue()
            }
            awaitUntilAsserted(timeout = 30) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-2")).isTrue()
            }
            println("Clusters created")
            println("Waiting for clusters...")
            awaitUntilAsserted(timeout = 360) {
                assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
            }
            awaitUntilAsserted(timeout = 360) {
                assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
            }
            println("Clusters started")
        }

        @AfterAll
        @JvmStatic
        fun removeFinalizers() {
            println("Removing finalizers...")
            removeFinalizers(name = "cluster-1")
            removeFinalizers(name = "cluster-2")
            deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-1.yaml")
            deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-2.yaml")
            awaitUntilAsserted(timeout = 360) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isFalse()
            }
            awaitUntilAsserted(timeout = 360) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-2")).isFalse()
            }
        }
    }

    @Test
    fun `should scale clusters up and down`() {
        println("Should rescale clusters...")
        val scaleOptions1 = ScaleOptions(taskManagers = 2)
        scaleCluster(name = "cluster-1", options = scaleOptions1, port = port)
        val scaleOptions2 = ScaleOptions(taskManagers = 1)
        scaleCluster(name = "cluster-2", options = scaleOptions2, port = port)
        awaitUntilAsserted(timeout = 30) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Scaling)).isTrue()
        }
        awaitUntilAsserted(timeout = 30) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Scaling)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
        }
        val response1 = getClusterStatus(name = "cluster-1", port = port)
        println(response1)
        assertThat(response1["status"] as String?).isEqualTo("COMPLETED")
        val status1 = JSON().deserialize<V1FlinkClusterStatus>(response1["output"] as String, statusTypeToken.type)
        assertThat(status1.taskManagers).isEqualTo(2)
        assertThat(status1.activeTaskManagers).isEqualTo(2)
        assertThat(status1.jobParallelism).isEqualTo(2)
        assertThat(status1.taskSlots).isEqualTo(1)
        assertThat(status1.totalTaskSlots).isEqualTo(2)
        val response2 = getClusterStatus(name = "cluster-2", port = port)
        println(response2)
        assertThat(response2["status"] as String?).isEqualTo("COMPLETED")
        val status2 = JSON().deserialize<V1FlinkClusterStatus>(response2["output"] as String, statusTypeToken.type)
        assertThat(status2.taskManagers).isEqualTo(1)
        assertThat(status2.activeTaskManagers).isEqualTo(1)
        assertThat(status2.jobParallelism).isEqualTo(2)
        assertThat(status2.taskSlots).isEqualTo(2)
        assertThat(status2.totalTaskSlots).isEqualTo(2)
        println("Clusters rescaled")
        println("Should scale down...")
        val scaleOptions0 = ScaleOptions(taskManagers = 0)
        scaleCluster(name = "cluster-1", options = scaleOptions0, port = port)
        scaleCluster(name = "cluster-2", options = scaleOptions0, port = port)
        awaitUntilAsserted(timeout = 30) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Scaling)).isTrue()
        }
        awaitUntilAsserted(timeout = 30) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Scaling)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Suspended)).isTrue()
            assertThat(hasActiveTaskManagers(redirect = redirect, namespace = namespace, name = "cluster-1", taskManagers = 0)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Suspended)).isTrue()
            assertThat(hasActiveTaskManagers(redirect = redirect, namespace = namespace, name = "cluster-2", taskManagers = 0)).isTrue()
        }
        val response3 = getClusterStatus(name = "cluster-1", port = port)
        println(response3)
        assertThat(response3["status"] as String?).isEqualTo("COMPLETED")
        val status3 = JSON().deserialize<V1FlinkClusterStatus>(response3["output"] as String, statusTypeToken.type)
        assertThat(status3.taskManagers).isEqualTo(0)
        assertThat(status3.activeTaskManagers).isEqualTo(0)
        assertThat(status3.jobParallelism).isEqualTo(0)
        assertThat(status3.taskSlots).isEqualTo(1)
        assertThat(status3.totalTaskSlots).isEqualTo(0)
        val response4 = getClusterStatus(name = "cluster-2", port = port)
        println(response4)
        assertThat(response4["status"] as String?).isEqualTo("COMPLETED")
        val status4 = JSON().deserialize<V1FlinkClusterStatus>(response4["output"] as String, statusTypeToken.type)
        assertThat(status4.taskManagers).isEqualTo(0)
        assertThat(status4.activeTaskManagers).isEqualTo(0)
        assertThat(status4.jobParallelism).isEqualTo(0)
        assertThat(status4.taskSlots).isEqualTo(2)
        assertThat(status4.totalTaskSlots).isEqualTo(0)
        println("Clusters rescaled")
    }
}