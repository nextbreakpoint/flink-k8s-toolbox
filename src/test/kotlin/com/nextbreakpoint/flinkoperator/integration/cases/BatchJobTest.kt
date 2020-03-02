package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

@Tag("IntegrationTest")
class BatchJobTest : IntegrationSetup() {
    companion object {
        @BeforeAll
        @JvmStatic
        fun createClusters() {
            println("Creating clusters...")
            createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-3.yaml")
            createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-4.yaml")
            awaitUntilAsserted(timeout = 30) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-3")).isTrue()
            }
            awaitUntilAsserted(timeout = 30) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-4")).isTrue()
            }
            println("Clusters created")
            println("Waiting for clusters...")
            awaitUntilAsserted(timeout = 360) {
                assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = ClusterStatus.Running)).isTrue()
            }
            awaitUntilAsserted(timeout = 360) {
                assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = ClusterStatus.Running)).isTrue()
            }
            println("Clusters started")
        }

        @AfterAll
        @JvmStatic
        fun removeFinalizers() {
            println("Removing finalizers...")
            deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-3.yaml")
            deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-4.yaml")
            removeFinalizers(name = "cluster-3")
            removeFinalizers(name = "cluster-4")
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
        println("Cluster should be suspended after batch job has finished")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = ClusterStatus.Suspended)).isTrue()
        }
        println("Cluster suspended")
        TimeUnit.SECONDS.sleep(10)
        awaitUntilAsserted(timeout = 20) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = ClusterStatus.Suspended)).isTrue()
        }
        println("Cluster still suspended")
    }

    @Test
    fun `should halt cluster when job failed`() {
        println("Cluster should fail when batch job fails")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = ClusterStatus.Failed)).isTrue()
        }
        println("Cluster failed")
        TimeUnit.SECONDS.sleep(10)
        awaitUntilAsserted(timeout = 20) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = ClusterStatus.Failed)).isTrue()
        }
        println("Cluster still failed")
    }
}