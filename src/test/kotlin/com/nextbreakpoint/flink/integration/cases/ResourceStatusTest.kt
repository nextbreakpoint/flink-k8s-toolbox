package com.nextbreakpoint.flink.integration.cases

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.integration.IntegrationSetup
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterStatus
import io.kubernetes.client.openapi.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("IntegrationTest")
class ResourceStatusTest : IntegrationSetup() {
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
        describeResources()
    }

    @Test
    fun `should return resource's status`() {
        println("Should create clusters...")
        createCluster(namespace = namespace, path = "integration/cluster-1.yaml")
        createCluster(namespace = namespace, path = "integration/cluster-2.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-1")).isTrue()
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-2")).isTrue()
        }
        println("Should start clusters...")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-1", status = ClusterStatus.Started)).isTrue()
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-2", status = ClusterStatus.Started)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-1", status = ResourceStatus.Updated)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updated)).isTrue()
        }
        println("Should start jobs...")
        awaitUntilAsserted(timeout = 240) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-1", status = JobStatus.Started)).isTrue()
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-2", status = JobStatus.Started)).isTrue()
        }
        println("Should return status...")
        val response1 = getClusterStatus(clusterName = "cluster-1", port = port)
        println(response1)
        assertThat(response1["status"] as String?).isEqualTo("OK")
        val status1 = JSON().deserialize<V2FlinkClusterStatus>(response1["output"] as String, clusterStatusTypeToken.type)
        assertThat(status1.supervisorStatus).isEqualTo(ClusterStatus.Started.toString())
        assertThat(status1.taskManagers).isEqualTo(1)
        assertThat(status1.taskManagerReplicas).isEqualTo(1)
        assertThat(status1.serviceMode).isEqualTo("NodePort")
        assertThat(status1.taskSlots).isEqualTo(1)
        assertThat(status1.totalTaskSlots).isEqualTo(1)
        val response2 = getClusterStatus(clusterName = "cluster-2", port = port)
        println(response2)
        assertThat(response2["status"] as String?).isEqualTo("OK")
        val status2 = JSON().deserialize<V2FlinkClusterStatus>(response2["output"] as String, clusterStatusTypeToken.type)
        assertThat(status2.supervisorStatus).isEqualTo(ClusterStatus.Started.toString())
        assertThat(status2.taskManagers).isEqualTo(2)
        assertThat(status2.taskManagerReplicas).isEqualTo(2)
        assertThat(status2.serviceMode).isEqualTo("ClusterIP")
        assertThat(status2.taskSlots).isEqualTo(2)
        assertThat(status2.totalTaskSlots).isEqualTo(4)
    }
}