package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import io.kubernetes.client.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.fail

@Tag("IntegrationTest")
class SavepointTest : IntegrationSetup() {
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
            awaitUntilAsserted(timeout = 30) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isFalse()
            }
            awaitUntilAsserted(timeout = 30) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-2")).isFalse()
            }
        }
    }

    @Test
    fun `should create savepoint periodically`() {
        println("Should create savepoint automatically...")
        val response = getClusterStatus(name = "cluster-2", port = port)
        println(response)
        assertThat(response["status"] as String?).isEqualTo("COMPLETED")
        val initialStatus = JSON().deserialize<V1FlinkClusterStatus>(response["output"] as String, statusTypeToken.type)
        assertThat(initialStatus.savepointMode).isEqualTo("Manual")
        if (updateCluster(redirect = redirect, namespace = namespace, name = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/operator/savepointMode\",\"value\":\"Automatic\"}]") != 0) {
            fail("Can't update cluster")
        }
        awaitUntilAsserted(timeout = 120) {
            val pollResponse = getClusterStatus(name = "cluster-2", port = port)
            println(pollResponse)
            assertThat(pollResponse["status"] as String?).isEqualTo("COMPLETED")
            val latestStatus = JSON().deserialize<V1FlinkClusterStatus>(pollResponse["output"] as String, statusTypeToken.type)
            assertThat(latestStatus.savepointPath).isNotBlank()
            assertThat(latestStatus.savepointTimestamp).isNotNull()
        }
        val nextResponse = getClusterStatus(name = "cluster-2", port = port)
        println(nextResponse)
        assertThat(nextResponse["status"] as String?).isEqualTo("COMPLETED")
        val currentStatus = JSON().deserialize<V1FlinkClusterStatus>(nextResponse["output"] as String, statusTypeToken.type)
        assertThat(currentStatus.savepointPath).isNotBlank()
        assertThat(currentStatus.savepointTimestamp).isNotNull()
        awaitUntilAsserted(timeout = 120) {
            val pollResponse = getClusterStatus(name = "cluster-2", port = port)
            println(pollResponse)
            assertThat(pollResponse["status"] as String?).isEqualTo("COMPLETED")
            val latestStatus = JSON().deserialize<V1FlinkClusterStatus>(pollResponse["output"] as String, statusTypeToken.type)
            assertThat(latestStatus.savepointPath).isNotBlank()
            assertThat(latestStatus.savepointTimestamp).isNotNull()
            assertThat(latestStatus.savepointPath).isNotEqualTo(currentStatus.savepointPath)
            assertThat(latestStatus.savepointTimestamp).isGreaterThan(currentStatus.savepointTimestamp)
        }
    }

    @Test
    fun `should create savepoint manually`() {
        println("Should create savepoint manually...")
        val response = getClusterStatus(name = "cluster-1", port = port)
        println(response)
        assertThat(response["status"] as String?).isEqualTo("COMPLETED")
        val initialStatus = JSON().deserialize<V1FlinkClusterStatus>(response["output"] as String, statusTypeToken.type)
        assertThat(initialStatus.savepointMode).isEqualTo("Manual")
        assertThat(initialStatus.savepointPath).isBlank()
        assertThat(initialStatus.savepointTimestamp).isNull()
        triggerSavepoint(name = "cluster-1", port = port)
        awaitUntilAsserted(timeout = 120) {
            val pollResponse = getClusterStatus(name = "cluster-1", port = port)
            println(pollResponse)
            assertThat(pollResponse["status"] as String?).isEqualTo("COMPLETED")
            val latestStatus = JSON().deserialize<V1FlinkClusterStatus>(pollResponse["output"] as String, statusTypeToken.type)
            assertThat(latestStatus.savepointPath).isNotBlank()
            assertThat(latestStatus.savepointTimestamp).isNotNull()
        }
    }
}