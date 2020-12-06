package com.nextbreakpoint.flink.integration.cases

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.integration.IntegrationSetup
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobStatus
import io.kubernetes.client.openapi.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.fail

@Tag("IntegrationTest")
class ResourceUpdatedTest : IntegrationSetup() {
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
    fun `should update cluster after patching resource spec`() {
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

        val response0 = getClusterStatus(clusterName = "cluster-0", port = port)
        assertThat(response0["status"] as String?).isEqualTo("OK")
        val status0 = JSON().deserialize<V1FlinkClusterStatus>(response0["output"] as String, clusterStatusTypeToken.type)
        assertThat(status0.serviceMode).isEqualTo("NodePort")

        println("Should update clusters...")
        if (updateDeployment(namespace = namespace, name = "cluster-0", patch = "[{\"op\":\"replace\",\"path\":\"/spec/cluster/jobManager/serviceMode\",\"value\":\"ClusterIP\"}]") != 0) {
            fail("Can't update deployment")
        }
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasClusterStatus(namespace = namespace, name = "cluster-0", status = ClusterStatus.Stopping)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, name = "cluster-0", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updated)).isTrue()
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 1)).isTrue()
        }

        val response1 = getClusterStatus(clusterName = "cluster-0", port = port)
        assertThat(response1["status"] as String?).isEqualTo("OK")
        val status1 = JSON().deserialize<V1FlinkClusterStatus>(response1["output"] as String, clusterStatusTypeToken.type)
        assertThat(status1.serviceMode).isEqualTo("ClusterIP")

        awaitUntilAsserted(timeout = 60) {
            val response = getClusterStatus(clusterName = "cluster-0", port = port)
            assertThat(response["status"] as String?).isEqualTo("OK")
            val status = JSON().deserialize<V1FlinkClusterStatus>(response["output"] as String, clusterStatusTypeToken.type)
            assertThat(status.taskSlots).isEqualTo(2)
            assertThat(status.totalTaskSlots).isEqualTo(2)
        }

        if (updateDeployment(namespace = namespace, name = "cluster-0", patch = "[{\"op\":\"replace\",\"path\":\"/spec/cluster/taskManager/taskSlots\",\"value\":4}]") != 0) {
            fail("Can't update deployment")
        }
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasClusterStatus(namespace = namespace, name = "cluster-0", status = ClusterStatus.Stopping)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, name = "cluster-0", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updated)).isTrue()
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 1)).isTrue()
        }

        awaitUntilAsserted(timeout = 60) {
            val response = getClusterStatus(clusterName = "cluster-0", port = port)
            assertThat(response["status"] as String?).isEqualTo("OK")
            val status = JSON().deserialize<V1FlinkClusterStatus>(response["output"] as String, clusterStatusTypeToken.type)
            assertThat(status.taskSlots).isEqualTo(4)
            assertThat(status.totalTaskSlots).isEqualTo(4)
        }

        if (updateCluster(namespace = namespace, name = "cluster-0", patch = "[{\"op\":\"replace\",\"path\":\"/spec/taskManagers\",\"value\":2}]") != 0) {
            fail("Can't update deployment")
        }
        awaitUntilAsserted(timeout = 60) {
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
            assertThat(status.taskSlots).isEqualTo(4)
            assertThat(status.totalTaskSlots).isEqualTo(8)
        }
    }

    @Test
    fun `should update job after patching resource spec`() {
        println("Should create clusters...")
        createResource(namespace = namespace, path = "integration/deployment-1.yaml")
        awaitUntilAsserted(timeout = 60) {
            assertThat(clusterExists(namespace = namespace, name = "cluster-1")).isTrue()
        }
        println("Should start clusters...")
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, name = "cluster-1", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-1", status = ResourceStatus.Updated)).isTrue()
        }
        println("Should start jobs...")
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, name = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
        }

        val response0 = getJobStatus(clusterName = "cluster-1", jobName = "job-0", port = port)
        assertThat(response0["status"] as String?).isEqualTo("OK")
        val status0 = JSON().deserialize<V1FlinkJobStatus>(response0["output"] as String, jobStatusTypeToken.type)
//        assertThat(status0.bootstrap.arguments).isEqualTo(listOf("--CONSOLE_OUTPUT", "true"))

        println("Should update jobs...")
        if (updateDeployment(namespace = namespace, name = "cluster-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/bootstrap/arguments\",\"value\":[]}]") != 0) {
            fail("Can't update deployment")
        }
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasJobStatus(namespace = namespace, name = "cluster-1-job-0", status = JobStatus.Stopping)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasJobStatus(namespace = namespace, name = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fj", name = "cluster-1-job-0", status = ResourceStatus.Updated)).isTrue()
        }

        val response1 = getJobStatus(clusterName = "cluster-1", jobName = "job-0", port = port)
        assertThat(response1["status"] as String?).isEqualTo("OK")
        val status1 = JSON().deserialize<V1FlinkJobStatus>(response1["output"] as String, jobStatusTypeToken.type)
//        assertThat(status1.bootstrap.arguments).isEmpty()

        println("Should update jobs...")
        if (updateDeployment(namespace = namespace, name = "cluster-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/bootstrap/arguments\",\"value\":[\"--TEST=true\"]}]") != 0) {
            fail("Can't update deployment")
        }
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasJobStatus(namespace = namespace, name = "cluster-1-job-0", status = JobStatus.Stopping)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasJobStatus(namespace = namespace, name = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fj", name = "cluster-1-job-0", status = ResourceStatus.Updated)).isTrue()
        }

        val response2 = getJobStatus(clusterName = "cluster-1", jobName = "job-0", port = port)
        assertThat(response2["status"] as String?).isEqualTo("OK")
        val status2 = JSON().deserialize<V1FlinkJobStatus>(response2["output"] as String, jobStatusTypeToken.type)
//        assertThat(status2.bootstrap.arguments).isEqualTo(listOf("--TEST=true"))
    }
}