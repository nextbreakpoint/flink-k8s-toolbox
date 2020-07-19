package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

@Tag("IntegrationTest")
class BatchJobTest : IntegrationSetup() {
    companion object {
        @AfterAll
        @JvmStatic
        fun removeFinalizers() {
            println("Removing finalizers...")
            removeFinalizers(name = "cluster-3")
            removeFinalizers(name = "cluster-4")
            deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-3.yaml")
            deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-4.yaml")
            awaitUntilAsserted(timeout = 360) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-3")).isFalse()
            }
            awaitUntilAsserted(timeout = 360) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-4")).isFalse()
            }
        }
    }

    @Test
    fun `should suspend cluster when job finished`() {
        println("Creating cluster...")
        createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-3.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-3")).isTrue()
        }
        println("Cluster created")
        println("Waiting for cluster...")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = ClusterStatus.Running)).isTrue()
        }
        println("Cluster started")
        println("Cluster should status be finished after batch job has finished")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = ClusterStatus.Finished)).isTrue()
        }
        println("Cluster finished")
    }

    @Test
    fun `should fail cluster when job failed`() {
        println("Creating cluster...")
        createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-4.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-4")).isTrue()
        }
        println("Cluster created")
        println("Waiting for cluster...")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = ClusterStatus.Running)).isTrue()
        }
        println("Cluster started")
        println("Cluster should status be failed after batch job has failed")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = ClusterStatus.Failed)).isTrue()
        }
        println("Cluster failed")
    }
}