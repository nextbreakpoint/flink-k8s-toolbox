package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterStopTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val operatorCache = mock(Cache::class.java)
    private val command = ClusterStop(flinkOptions, flinkContext, kubernetesContext, operatorCache)

    @BeforeEach
    fun configure() {
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.appendTasks(cluster, listOf(ClusterTask.ClusterRunning))
        given(operatorCache.getFlinkCluster(eq(clusterId))).thenReturn(cluster)
    }

    @Test
    fun `should fail when cluster doesn't exist`() {
        given(operatorCache.getFlinkCluster(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isEmpty()
    }

    @Test
    fun `should return expected result when operator is not idle`() {
        cluster.spec.bootstrap = null
        Status.setTaskStatus(cluster, TaskStatus.Awaiting)
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when operator status is not running and resources are not deleted`() {
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.setClusterStatus(cluster, ClusterStatus.Stopping)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when cluster is running, job is not defined and resources must be deleted`() {
        cluster.spec.bootstrap = null
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
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
        cluster.spec.bootstrap = null
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.SuspendCluster,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when cluster is not running and job is not defined`() {
        cluster.spec.bootstrap = null
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when cluster is running, savepoint is disabled and resources must be deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
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
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
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
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
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
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
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
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
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
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
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
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
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
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
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
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when cluster is checkpointing and resources are not deleted`() {
        Status.setClusterStatus(cluster, ClusterStatus.Checkpointing)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.ClusterRunning
        ))
    }
}