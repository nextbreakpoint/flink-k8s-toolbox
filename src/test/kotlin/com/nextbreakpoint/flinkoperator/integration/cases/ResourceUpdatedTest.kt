package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import io.kubernetes.client.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
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

    @BeforeEach
    fun createClusters() {
        println("Creating clusters...")
        createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-1.yaml")
        createCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-2.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isTrue()
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

    @AfterEach
    fun deleteClusters() {
        println("Deleting clusters...")
        deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-1.yaml")
        deleteCluster(redirect = redirect, namespace = namespace, path = "integration/cluster-2.yaml")
        awaitUntilCondition(timeout = 360) {
            hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Terminated)
        }
        awaitUntilCondition(timeout = 360) {
            hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Terminated)
        }
        println("Clusters deleted")
        // we must terminate the clusters otherwise some resources might stick around and cause next test to fail.
        // we should remove any cluster resource just in case, to make sure nothing is left after each test
        println("Removing finalizers...")
        removeFinalizers(name = "cluster-1")
        removeFinalizers(name = "cluster-2")
        awaitUntilAsserted(timeout = 360) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-1")).isFalse()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-2")).isFalse()
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
        val beforeResponse1 = getClusterStatus(name = "cluster-1", port = port)
        println(beforeResponse1)
        assertThat(beforeResponse1["status"] as String?).isEqualTo("OK")
        val beforeStatus1 = JSON().deserialize<V1FlinkClusterStatus>(beforeResponse1["output"] as String, statusTypeToken.type)
        assertThat(beforeStatus1.serviceMode).isEqualTo("NodePort")
        val beforeResponse2 = getClusterStatus(name = "cluster-2", port = port)
        println(beforeResponse2)
        assertThat(beforeResponse2["status"] as String?).isEqualTo("OK")
        val beforeStatus2 = JSON().deserialize<V1FlinkClusterStatus>(beforeResponse2["output"] as String, statusTypeToken.type)
        assertThat(beforeStatus2.taskSlots).isEqualTo(2)
        assertThat(beforeStatus2.totalTaskSlots).isEqualTo(4)
        println("Should update clusters...")
        if (updateCluster(redirect = redirect, namespace = namespace, name = "cluster-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobManager/serviceMode\",\"value\":\"ClusterIP\"}]") != 0) {
            fail("Can't update cluster")
        }
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Restarting)).isTrue()
        }
        awaitUntilAsserted(timeout = 180, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 240) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Starting)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
            assertThat(hasActiveTaskManagers(redirect = redirect, namespace = namespace, name = "cluster-1", taskManagers = 1)).isTrue()
        }
        if (updateCluster(redirect = redirect, namespace = namespace, name = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/taskManager/taskSlots\",\"value\":1}]") != 0) {
            fail("Can't update cluster")
        }
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Restarting)).isTrue()
        }
        awaitUntilAsserted(timeout = 180, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 240) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Starting)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
            assertThat(hasActiveTaskManagers(redirect = redirect, namespace = namespace, name = "cluster-2", taskManagers = 2)).isTrue()
        }
        val response1 = getClusterStatus(name = "cluster-1", port = port)
        println(response1)
        assertThat(response1["status"] as String?).isEqualTo("OK")
        val status1 = JSON().deserialize<V1FlinkClusterStatus>(response1["output"] as String, statusTypeToken.type)
        assertThat(status1.serviceMode).isEqualTo("ClusterIP")
        val response2 = getClusterStatus(name = "cluster-2", port = port)
        println(response2)
        assertThat(response2["status"] as String?).isEqualTo("OK")
        val status2 = JSON().deserialize<V1FlinkClusterStatus>(response2["output"] as String, statusTypeToken.type)
        assertThat(status2.taskSlots).isEqualTo(1)
        assertThat(status2.totalTaskSlots).isEqualTo(2)
        TimeUnit.SECONDS.sleep(10)
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
        }
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
        }
        println("Clusters updated")
    }

    @Test
    fun `should update job after patching bootstrap spec`() {
        val beforeResponse1 = getClusterStatus(name = "cluster-1", port = port)
        println(beforeResponse1)
        assertThat(beforeResponse1["status"] as String?).isEqualTo("OK")
        val beforeStatus1 = JSON().deserialize<V1FlinkClusterStatus>(beforeResponse1["output"] as String, statusTypeToken.type)
        assertThat(beforeStatus1.bootstrap.arguments).isEqualTo(listOf("--CONSOLE_OUTPUT", "true"))
        val beforeResponse2 = getClusterStatus(name = "cluster-2", port = port)
        println(beforeResponse2)
        assertThat(beforeResponse2["status"] as String?).isEqualTo("OK")
        val beforeStatus2 = JSON().deserialize<V1FlinkClusterStatus>(beforeResponse2["output"] as String, statusTypeToken.type)
        assertThat(beforeStatus2.bootstrap.arguments).isEqualTo(listOf("--CONSOLE_OUTPUT", "true"))
        println("Should update clusters...")
        if (updateCluster(redirect = redirect, namespace = namespace, name = "cluster-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/bootstrap/arguments\",\"value\":[]}]") != 0) {
            fail("Can't update cluster")
        }
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Restarting)).isTrue()
        }
        awaitUntilAsserted(timeout = 180, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 240) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Starting)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
        }
        if (updateCluster(redirect = redirect, namespace = namespace, name = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/bootstrap/arguments\",\"value\":[\"--TEST=true\"]}]") != 0) {
            fail("Can't update cluster")
        }
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Restarting)).isTrue()
        }
        awaitUntilAsserted(timeout = 180, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 240) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Starting)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
        }
        val response1 = getClusterStatus(name = "cluster-1", port = port)
        println(response1)
        assertThat(response1["status"] as String?).isEqualTo("OK")
        val status1 = JSON().deserialize<V1FlinkClusterStatus>(response1["output"] as String, statusTypeToken.type)
        assertThat(status1.bootstrap.arguments).isEmpty()
        val response2 = getClusterStatus(name = "cluster-2", port = port)
        println(response2)
        assertThat(response2["status"] as String?).isEqualTo("OK")
        val status2 = JSON().deserialize<V1FlinkClusterStatus>(response2["output"] as String, statusTypeToken.type)
        assertThat(status2.bootstrap.arguments).isEqualTo(listOf("--TEST=true"))
        TimeUnit.SECONDS.sleep(10)
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Running)).isTrue()
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Running)).isTrue()
        }
        println("Clusters updated")
    }

    @Test
    fun `should cluster fail after patching bootstrap spec with broken configuration`() {
        println("Should update clusters...")
        if (updateCluster(redirect = redirect, namespace = namespace, name = "cluster-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/bootstrap/image\",\"value\":\"integration/wrongimage\"}]") != 0) {
            fail("Can't update cluster")
        }
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Restarting)).isTrue()
        }
        awaitUntilAsserted(timeout = 180, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 240) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Starting)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-1", status = ClusterStatus.Failed)).isTrue()
        }
        if (updateCluster(redirect = redirect, namespace = namespace, name = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/bootstrap/className\",\"value\":\"wrongclassname\"}]") != 0) {
            fail("Can't update cluster")
        }
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Restarting)).isTrue()
        }
        awaitUntilAsserted(timeout = 180, delay = 1, interval = 1) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 240) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Starting)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(redirect = redirect, namespace = namespace, name = "cluster-2", status = ClusterStatus.Failed)).isTrue()
        }
        println("Clusters updated. The cluster failed as expected because of invalid bootstrap configuration")
    }
}