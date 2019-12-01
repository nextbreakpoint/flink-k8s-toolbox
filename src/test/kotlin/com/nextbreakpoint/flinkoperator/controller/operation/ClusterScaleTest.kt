package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.CacheAdapter
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterScaleTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val adapter = CacheAdapter(cluster, CachedResources())
    private val command = ClusterScale(flinkOptions, flinkClient, kubeClient, adapter)

    @BeforeEach
    fun configure() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.setBootstrap(cluster, cluster.spec.bootstrap)
        Status.appendTasks(cluster, listOf(ClusterTask.ClusterRunning))
    }

    @Test
    fun `should return expected result when operator is idle but cluster is not running`() {
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        Status.resetTasks(cluster, listOf(ClusterTask.ClusterHalted))
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 4, taskSlots = 2))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.RETRY)
        assertThat(result.output).isEmpty()
    }

    @Test
    fun `should return expected result when job is not defined and cluster is running`() {
        Status.setBootstrap(cluster,null)
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 4, taskSlots = 2))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(
            listOf(
                ClusterTask.RescaleCluster,
                ClusterTask.ClusterRunning
            )
        )
    }

    @Test
    fun `should return expected result when job is not defined and cluster is running and no task managers`() {
        Status.setBootstrap(cluster,null)
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 0, taskSlots = 2))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(
            listOf(
                ClusterTask.StoppingCluster,
                ClusterTask.TerminatePods,
                ClusterTask.SuspendCluster,
                ClusterTask.ClusterHalted
            )
        )
    }

    @Test
    fun `should return expected result when job is defined and cluster is running`() {
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 4, taskSlots = 2))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(
            listOf(
                ClusterTask.StoppingCluster,
                ClusterTask.CancelJob,
                ClusterTask.RescaleCluster,
                ClusterTask.StartingCluster,
                ClusterTask.CreateBootstrapJob,
                ClusterTask.ClusterRunning
            )
        )
    }

    @Test
    fun `should return expected result when job is defined and cluster is running and no task managers`() {
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 0, taskSlots = 2))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(
            listOf(
                ClusterTask.StoppingCluster,
                ClusterTask.CancelJob,
                ClusterTask.TerminatePods,
                ClusterTask.SuspendCluster,
                ClusterTask.ClusterHalted
            )
        )
    }
}
