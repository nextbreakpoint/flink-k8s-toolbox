package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
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

class ClusterStopTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val adapter = CacheAdapter(cluster, CachedResources())
    private val command = ClusterStop(flinkOptions, flinkClient, kubeClient, adapter)

    @BeforeEach
    fun configure() {
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.setBootstrap(cluster, cluster.spec.bootstrap)
        Status.appendTasks(cluster, listOf(ClusterTask.ClusterRunning))
    }

    @Test
    fun `should return expected result when cluster is running, job is not defined and resources must be deleted`() {
        Status.setBootstrap(cluster,null)
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.DeleteResources,
            ClusterTask.TerminatedCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is running, job is not defined and resources are not deleted`() {
        Status.setBootstrap(cluster,null)
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.SuspendCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is not running and job is not defined`() {
        Status.setBootstrap(cluster,null)
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.DeleteResources,
            ClusterTask.TerminatedCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is failed and job is not defined`() {
        Status.setBootstrap(cluster,null)
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.DeleteResources,
            ClusterTask.TerminatedCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is running, savepoint is disabled and resources must be deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.StopJob,
            ClusterTask.TerminatePods,
            ClusterTask.DeleteResources,
            ClusterTask.TerminatedCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is running, savepoint is disabled and resources are not deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.StopJob,
            ClusterTask.TerminatePods,
            ClusterTask.SuspendCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is suspended, savepoint is disabled and resources must be deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.DeleteResources,
            ClusterTask.TerminatedCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster has failed, savepoint is disabled and resources must be deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.DeleteResources,
            ClusterTask.TerminatedCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is running, savepoint is enabled and resources must be deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.CancelJob,
            ClusterTask.TerminatePods,
            ClusterTask.DeleteResources,
            ClusterTask.TerminatedCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is running, savepoint is enabled and resources are not deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = false))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.CancelJob,
            ClusterTask.TerminatePods,
            ClusterTask.SuspendCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is suspended, savepoint is enabled and resources must be deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.DeleteResources,
            ClusterTask.TerminatedCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster has failed, savepoint is enabled and resources must be deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.DeleteResources,
            ClusterTask.TerminatedCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is checkpointing and resources must be deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Checkpointing)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.RETRY)
        assertThat(result.output).isEmpty()
    }

    @Test
    fun `should return expected result when cluster is checkpointing and resources are not deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Checkpointing)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = false))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.RETRY)
        assertThat(result.output).isEmpty()
    }
}