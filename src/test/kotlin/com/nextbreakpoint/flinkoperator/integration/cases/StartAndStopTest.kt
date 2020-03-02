package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("IntegrationTest")
class StartAndStopTest : IntegrationSetup() {
    companion object {
        @BeforeAll
        @JvmStatic
        fun createClusters() {
            println("Creating clusters...")
            createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-1.yaml")
            createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-2.yaml")
            awaitUntilAsserted(timeout = 30) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isTrue()
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-2")).isTrue()
            }
            println("Clusters created")
        }

//        @AfterAll
//        @JvmStatic
//        fun deleteClusters() {
//            println("Deleting clusters...")
//            deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-1.yaml")
//            deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-2.yaml")
//            awaitUntilAsserted(timeout = 300) {
//                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isFalse()
//                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-2")).isFalse()
//            }
//            println("Clusters deleted")
//        }

        @AfterAll
        @JvmStatic
        fun removeFinalizers() {
            println("Removing finalizers...")
            removeFinalizers(name = "cluster-1")
            removeFinalizers(name = "cluster-2")
        }
    }

    @Test
    fun `should start and stop clusters`() {
        println("Should start clusters automatically...")
        awaitUntilAsserted(timeout = 300) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
        }
        println("Clusters started")
        println("Should suspend clusters...")
        val stopOptions = StopOptions(withoutSavepoint = false, deleteResources = false)
        stopCluster(name = "cluster-1", options = stopOptions, port = port)
        stopCluster(name = "cluster-2", options = stopOptions, port = port)
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Suspended)).isTrue()
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Suspended)).isTrue()
        }
        println("Clusters suspended")
        println("Should resume clusters...")
        val startOptions = StartOptions(withoutSavepoint = false)
        val startWithoutSavepointOptions = StartOptions(withoutSavepoint = true)
        startCluster(name = "cluster-1", options = startOptions, port = port)
        startCluster(name = "cluster-2", options = startWithoutSavepointOptions, port = port)
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
        }
        println("Clusters resumed")
        println("Should terminate clusters...")
        val terminateOptions = StopOptions(withoutSavepoint = false, deleteResources = true)
        val terminateWithoutSavepointOptions = StopOptions(withoutSavepoint = true, deleteResources = true)
        stopCluster(name = "cluster-1", options = terminateOptions, port = port)
        stopCluster(name = "cluster-2", options = terminateWithoutSavepointOptions, port = port)
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Terminated)).isTrue()
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Terminated)).isTrue()
        }
        println("Clusters terminated")
    }
}