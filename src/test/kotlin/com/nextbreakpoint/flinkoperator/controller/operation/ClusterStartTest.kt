package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorState
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

class ClusterStartTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val operatorCache = mock(OperatorCache::class.java)
    private val command = ClusterStart(flinkOptions, flinkContext, kubernetesContext, operatorCache)

    @BeforeEach
    fun configure() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorState.setTaskStatus(cluster, TaskStatus.Idle)
        OperatorState.appendTasks(cluster, listOf(OperatorTask.ClusterHalted))
        given(operatorCache.getFlinkCluster(eq(clusterId))).thenReturn(cluster)
    }

    @Test
    fun `should fail when cluster doesn't exist`() {
        given(operatorCache.getFlinkCluster(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isEmpty()
    }

    @Test
    fun `should return expected result when job is not defined and cluster is terminated but operator is not idle`() {
        cluster.spec.flinkJob = null
        OperatorState.setTaskStatus(cluster, TaskStatus.Awaiting)
        OperatorState.setClusterStatus(cluster, ClusterStatus.Terminated)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster is terminated and savepoint is enabled`() {
        cluster.spec.flinkJob = null
        OperatorState.setClusterStatus(cluster, ClusterStatus.Terminated)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.StartingCluster,
            OperatorTask.CreateResources,
            OperatorTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster is suspended and savepoint is enabled`() {
        cluster.spec.flinkJob = null
        OperatorState.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.StartingCluster,
            OperatorTask.RestartPods,
            OperatorTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster has failed and savepoint is enabled`() {
        cluster.spec.flinkJob = null
        OperatorState.setClusterStatus(cluster, ClusterStatus.Failed)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.StoppingCluster,
            OperatorTask.TerminatePods,
            OperatorTask.DeleteResources,
            OperatorTask.StartingCluster,
            OperatorTask.CreateResources,
            OperatorTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster is checkpointing and savepoint is enabled`() {
        cluster.spec.flinkJob = null
        OperatorState.setClusterStatus(cluster, ClusterStatus.Checkpointing)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is terminated and savepoint is enabled`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Terminated)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.StartingCluster,
            OperatorTask.CreateResources,
            OperatorTask.DeleteUploadJob,
            OperatorTask.CreateUploadJob,
            OperatorTask.EraseSavepoint,
            OperatorTask.StartJob,
            OperatorTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is suspended and savepoint is enabled`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.StartingCluster,
            OperatorTask.RestartPods,
            OperatorTask.DeleteUploadJob,
            OperatorTask.CreateUploadJob,
            OperatorTask.EraseSavepoint,
            OperatorTask.StartJob,
            OperatorTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster has failed and savepoint is enabled`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Failed)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.StoppingCluster,
            OperatorTask.DeleteUploadJob,
            OperatorTask.TerminatePods,
            OperatorTask.DeleteResources,
            OperatorTask.StartingCluster,
            OperatorTask.CreateResources,
            OperatorTask.CreateUploadJob,
            OperatorTask.EraseSavepoint,
            OperatorTask.StartJob,
            OperatorTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is checkpointing and savepoint is enabled`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Checkpointing)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.ClusterHalted
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is terminated and savepoint is disabled`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Terminated)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.StartingCluster,
            OperatorTask.CreateResources,
            OperatorTask.DeleteUploadJob,
            OperatorTask.CreateUploadJob,
            OperatorTask.StartJob,
            OperatorTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is suspended and savepoint is disabled`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.StartingCluster,
            OperatorTask.RestartPods,
            OperatorTask.DeleteUploadJob,
            OperatorTask.CreateUploadJob,
            OperatorTask.StartJob,
            OperatorTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster has failed and savepoint is disabled`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Failed)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.StoppingCluster,
            OperatorTask.DeleteUploadJob,
            OperatorTask.TerminatePods,
            OperatorTask.DeleteResources,
            OperatorTask.StartingCluster,
            OperatorTask.CreateResources,
            OperatorTask.CreateUploadJob,
            OperatorTask.StartJob,
            OperatorTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is checkpointing and savepoint is disabled`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Checkpointing)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.ClusterHalted
        ))
    }
}