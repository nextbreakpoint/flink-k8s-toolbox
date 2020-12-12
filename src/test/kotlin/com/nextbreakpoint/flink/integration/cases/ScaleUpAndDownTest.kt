package com.nextbreakpoint.flink.integration.cases

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.integration.IntegrationSetup
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterStatus
import io.kubernetes.client.openapi.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("IntegrationTest")
class ScaleUpAndDownTest : IntegrationSetup() {
    companion object {
        @BeforeAll
        @JvmStatic
        fun setup() {
            IntegrationSetup.setup()
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
        printJobManagerLogs()
        printTaskManagerLogs()
        printBootstrapJobLogs()
        describeResources()
    }

    @Test
    fun `should scale clusters up and down`() {
        println("Should create clusters...")
        createResource(namespace = namespace, path = "integration/deployment-0.yaml")
        awaitUntilAsserted(timeout = 60) {
            assertThat(clusterExists(namespace = namespace, name = "cluster-0")).isTrue()
        }
        println("Should start clusters...")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, name = "cluster-0", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updated)).isTrue()
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 1)).isTrue()
        }

        println("Should scale up...")
        val scaleOptions1 = ScaleClusterOptions(taskManagers = 2)
        scaleCluster(clusterName = "cluster-0", options = scaleOptions1, port = port)
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updated)).isTrue()
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 2)).isTrue()
        }

        awaitUntilAsserted(timeout = 60) {
            val response = getClusterStatus(clusterName = "cluster-0", port = port)
            assertThat(response["status"] as String?).isEqualTo("OK")
            val status = JSON().deserialize<V1FlinkClusterStatus>(response["output"] as String, clusterStatusTypeToken.type)
            assertThat(status.taskManagers).isEqualTo(2)
            assertThat(status.taskManagerReplicas).isEqualTo(2)
            assertThat(status.taskSlots).isEqualTo(2)
            assertThat(status.totalTaskSlots).isEqualTo(4)
        }

        println("Should scale down...")
        val scaleOptions0 = ScaleClusterOptions(taskManagers = 0)
        scaleCluster(clusterName = "cluster-0", options = scaleOptions0, port = port)
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updated)).isTrue()
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 0)).isTrue()
        }

        awaitUntilAsserted(timeout = 60) {
            val response = getClusterStatus(clusterName = "cluster-0", port = port)
            assertThat(response["status"] as String?).isEqualTo("OK")
            val status = JSON().deserialize<V1FlinkClusterStatus>(response["output"] as String, clusterStatusTypeToken.type)
            assertThat(status.taskManagers).isEqualTo(0)
            assertThat(status.taskManagerReplicas).isEqualTo(0)
            assertThat(status.taskSlots).isEqualTo(2)
            assertThat(status.totalTaskSlots).isEqualTo(0)
        }
    }
}