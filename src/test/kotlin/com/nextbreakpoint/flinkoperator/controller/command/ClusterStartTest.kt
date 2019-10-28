package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
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
    private val cluster = TestFactory.aCluster("test", "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val operatorCache = mock(OperatorCache::class.java)
    private val command = ClusterStart(flinkOptions, flinkContext, kubernetesContext, operatorCache)

    @BeforeEach
    fun configure() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.IDLE)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.CLUSTER_HALTED))
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
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.AWAITING)
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.TERMINATED)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster is terminated and savepoint is enabled`() {
        cluster.spec.flinkJob = null
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.TERMINATED)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STARTING_CLUSTER,
            OperatorTask.CREATE_RESOURCES,
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster is suspended and savepoint is enabled`() {
        cluster.spec.flinkJob = null
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.SUSPENDED)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STARTING_CLUSTER,
            OperatorTask.RESTART_PODS,
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster has failed and savepoint is enabled`() {
        cluster.spec.flinkJob = null
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.FAILED)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
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
            OperatorTask.STARTING_CLUSTER,
            OperatorTask.CREATE_RESOURCES,
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster is checkpointing and savepoint is enabled`() {
        cluster.spec.flinkJob = null
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.CHECKPOINTING)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is terminated and savepoint is enabled`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.TERMINATED)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STARTING_CLUSTER,
            OperatorTask.CREATE_RESOURCES,
            OperatorTask.DELETE_UPLOAD_JOB,
            OperatorTask.UPLOAD_JAR,
            OperatorTask.ERASE_SAVEPOINT,
            OperatorTask.START_JOB,
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is suspended and savepoint is enabled`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.SUSPENDED)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STARTING_CLUSTER,
            OperatorTask.RESTART_PODS,
            OperatorTask.DELETE_UPLOAD_JOB,
            OperatorTask.UPLOAD_JAR,
            OperatorTask.ERASE_SAVEPOINT,
            OperatorTask.START_JOB,
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster has failed and savepoint is enabled`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.FAILED)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.DELETE_UPLOAD_JOB,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.DELETE_RESOURCES,
            OperatorTask.STARTING_CLUSTER,
            OperatorTask.RESTART_PODS,
            OperatorTask.UPLOAD_JAR,
            OperatorTask.ERASE_SAVEPOINT,
            OperatorTask.START_JOB,
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is checkpointing and savepoint is enabled`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.CHECKPOINTING)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.CLUSTER_HALTED
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is terminated and savepoint is disabled`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.TERMINATED)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STARTING_CLUSTER,
            OperatorTask.CREATE_RESOURCES,
            OperatorTask.DELETE_UPLOAD_JOB,
            OperatorTask.UPLOAD_JAR,
            OperatorTask.START_JOB,
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is suspended and savepoint is disabled`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.SUSPENDED)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STARTING_CLUSTER,
            OperatorTask.RESTART_PODS,
            OperatorTask.DELETE_UPLOAD_JOB,
            OperatorTask.UPLOAD_JAR,
            OperatorTask.START_JOB,
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster has failed and savepoint is disabled`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.FAILED)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.STOPPING_CLUSTER,
            OperatorTask.DELETE_UPLOAD_JOB,
            OperatorTask.TERMINATE_PODS,
            OperatorTask.DELETE_RESOURCES,
            OperatorTask.STARTING_CLUSTER,
            OperatorTask.RESTART_PODS,
            OperatorTask.UPLOAD_JAR,
            OperatorTask.START_JOB,
            OperatorTask.CLUSTER_RUNNING
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is checkpointing and savepoint is disabled`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.CHECKPOINTING)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            OperatorTask.CLUSTER_HALTED
        ))
    }
}