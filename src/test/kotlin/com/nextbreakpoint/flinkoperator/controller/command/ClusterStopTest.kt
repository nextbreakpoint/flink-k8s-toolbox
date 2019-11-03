package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
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

class ClusterStopTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val operatorCache = mock(OperatorCache::class.java)
    private val command = ClusterStop(flinkOptions, flinkContext, kubernetesContext, operatorCache)

    @BeforeEach
    fun configure() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.TERMINATED)
        OperatorState.setTaskStatus(cluster, TaskStatus.IDLE)
        OperatorState.appendTasks(cluster, listOf(OperatorTask.CLUSTER_RUNNING))
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
        cluster.spec.flinkJob = null
        OperatorState.setTaskStatus(cluster, TaskStatus.AWAITING)
        OperatorState.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when operator status is not running and resources are not deleted`() {
        OperatorState.setTaskStatus(cluster, TaskStatus.IDLE)
        OperatorState.setClusterStatus(cluster, ClusterStatus.STOPPING)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when cluster is running, job is not defined and resources must be deleted`() {
        cluster.spec.flinkJob = null
        OperatorState.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.DELETE_RESOURCES,
            OperatorTask.TERMINATE_CLUSTER,
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when cluster is running, job is not defined and resources are not deleted`() {
        cluster.spec.flinkJob = null
        OperatorState.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.SUSPEND_CLUSTER,
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when cluster is running, savepoint is disabled and resources must be deleted`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.STOP_JOB,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.DELETE_RESOURCES,
            OperatorTask.TERMINATE_CLUSTER,
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when cluster is running, savepoint is disabled and resources are not deleted`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.STOP_JOB,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.SUSPEND_CLUSTER,
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when cluster is suspended, savepoint is disabled and resources must be deleted`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.SUSPENDED)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.DELETE_RESOURCES,
            OperatorTask.TERMINATE_CLUSTER,
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when cluster has failed, savepoint is disabled and resources must be deleted`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.DELETE_RESOURCES,
            OperatorTask.TERMINATE_CLUSTER,
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when cluster is running, savepoint is enabled and resources must be deleted`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.CANCEL_JOB,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.DELETE_RESOURCES,
            OperatorTask.TERMINATE_CLUSTER,
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when cluster is running, savepoint is enabled and resources are not deleted`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.CANCEL_JOB,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.SUSPEND_CLUSTER,
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when cluster is suspended, savepoint is enabled and resources must be deleted`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.SUSPENDED)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.DELETE_RESOURCES,
            OperatorTask.TERMINATE_CLUSTER,
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when cluster has failed, savepoint is enabled and resources must be deleted`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.DELETE_RESOURCES,
            OperatorTask.TERMINATE_CLUSTER,
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when cluster is checkpointing and resources must be deleted`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.CHECKPOINTING)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when cluster is checkpointing and resources are not deleted`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.CHECKPOINTING)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.CLUSTER_RUNNING
        ))
    }
}