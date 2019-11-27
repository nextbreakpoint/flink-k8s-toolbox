package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("IntegrationTest")
class StartAndStopTest : IntegrationSetup() {
    @BeforeEach
    fun createClusters() {
        println("Creating clusters...")
        createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-1.yaml")
        createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-2.yaml")
        awaitUntilAsserted(timeout = 60) {
            Assertions.assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isTrue()
        }
        awaitUntilAsserted(timeout = 60) {
            Assertions.assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-2")).isTrue()
        }
        println("Clusters created")
    }

    @AfterEach
    fun deleteClusters() {
        println("Deleting clusters...")
        deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-1.yaml")
        deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-2.yaml")
        awaitUntilAsserted(timeout = 60) {
            Assertions.assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isFalse()
        }
        awaitUntilAsserted(timeout = 60) {
            Assertions.assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-2")).isFalse()
        }
        println("Clusters deleted")
    }

    @Test
    fun shouldStartAndStopClusters() {
        println("Should start clusters automatically...")
        awaitUntilAsserted(timeout = 180) {
            Assertions.assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
        }
        awaitUntilAsserted(timeout = 180) {
            Assertions.assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
        }
        println("Clusters started")
//        println("Should suspend clusters...")
//        awaitUntilAsserted(timeout = 180) {
//            Assertions.assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isFalse()
//        }
//        awaitUntilAsserted(timeout = 180) {
//            Assertions.assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isFalse()
//        }
//        println("Clusters suspended")
    }
}