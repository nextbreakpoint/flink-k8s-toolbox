package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

@Tag("IntegrationTest")
class BatchJobTest : IntegrationSetup() {
    @Test
    fun `should start and then suspend cluster when job finished`() {
        println("Creating cluster...")
        createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-3.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-3")).isTrue()
        }
        println("Cluster created")
        println("Should start cluster automatically...")
        awaitUntilAsserted(timeout = 300) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = ClusterStatus.Running)).isTrue()
            assertThat(hasTaskStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = TaskStatus.Idle)).isTrue()
        }
        println("Cluster started")
        awaitUntilAsserted(timeout = 300) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = ClusterStatus.Suspended)).isTrue()
            assertThat(hasTaskStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = TaskStatus.Idle)).isTrue()
        }
        println("Cluster suspended")
        TimeUnit.SECONDS.sleep(10)
        awaitUntilAsserted(timeout = 20) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = ClusterStatus.Suspended)).isTrue()
            assertThat(hasTaskStatus(redirect = redirect, namespace = namespace, name = "cluster-3", status = TaskStatus.Idle)).isTrue()
        }
        describePods(redirect = redirect, namespace = namespace)
        describeClusters(redirect = redirect, namespace = namespace)
        println("Deleting cluster...")
        deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-3.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-3")).isFalse()
        }
        println("Cluster deleted")
    }

    @Test
    fun `should start and then halt cluster when job failed`() {
        println("Creating cluster...")
        createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-4.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-4")).isTrue()
        }
        println("Cluster created")
        println("Should start cluster automatically...")
        awaitUntilAsserted(timeout = 300) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = ClusterStatus.Running)).isTrue()
            assertThat(hasTaskStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = TaskStatus.Idle)).isTrue()
        }
        println("Cluster started")
        awaitUntilAsserted(timeout = 300) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = ClusterStatus.Failed)).isTrue()
            assertThat(hasTaskStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = TaskStatus.Idle)).isTrue()
        }
        println("Cluster failed")
        TimeUnit.SECONDS.sleep(10)
        awaitUntilAsserted(timeout = 20) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = ClusterStatus.Failed)).isTrue()
            assertThat(hasTaskStatus(redirect = redirect, namespace = namespace, name = "cluster-4", status = TaskStatus.Idle)).isTrue()
        }
        describePods(redirect = redirect, namespace = namespace)
        describeClusters(redirect = redirect, namespace = namespace)
        println("Deleting cluster...")
        deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-4.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-4")).isFalse()
        }
        println("Cluster deleted")
    }
}