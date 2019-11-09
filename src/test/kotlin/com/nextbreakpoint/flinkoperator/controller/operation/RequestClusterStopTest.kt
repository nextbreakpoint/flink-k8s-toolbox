package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito
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

class RequestClusterStopTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val operatorCache = mock(OperatorCache::class.java)
    private val command = RequestClusterStop(flinkOptions, flinkContext, kubernetesContext, operatorCache)

    @BeforeEach
    fun configure() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Terminated)
        OperatorState.setTaskStatus(cluster, TaskStatus.Idle)
        OperatorState.appendTasks(cluster, listOf(OperatorTask.ClusterRunning))
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
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when stopping without savepoint and not deleting resources`() {
        OperatorState.setTaskStatus(cluster, TaskStatus.Idle)
        OperatorState.setClusterStatus(cluster, ClusterStatus.Stopping)
        val actionTimestamp = OperatorAnnotations.getActionTimestamp(cluster)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verify(kubernetesContext, times(1)).updateAnnotations(eq(clusterId), KotlinMockito.any())
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(OperatorAnnotations.getManualAction(cluster)).isEqualTo(ManualAction.STOP)
        assertThat(OperatorAnnotations.isWithSavepoint(cluster)).isEqualTo(true)
        assertThat(OperatorAnnotations.isDeleteResources(cluster)).isEqualTo(false)
        assertThat(OperatorAnnotations.getActionTimestamp(cluster)).isNotEqualTo(actionTimestamp)
    }

    @Test
    fun `should return expected result when stopping with savepoint and deleting resources`() {
        OperatorState.setTaskStatus(cluster, TaskStatus.Idle)
        OperatorState.setClusterStatus(cluster, ClusterStatus.Stopping)
        val actionTimestamp = OperatorAnnotations.getActionTimestamp(cluster)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verify(kubernetesContext, times(1)).updateAnnotations(eq(clusterId), KotlinMockito.any())
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(OperatorAnnotations.getManualAction(cluster)).isEqualTo(ManualAction.STOP)
        assertThat(OperatorAnnotations.isWithSavepoint(cluster)).isEqualTo(false)
        assertThat(OperatorAnnotations.isDeleteResources(cluster)).isEqualTo(true)
        assertThat(OperatorAnnotations.getActionTimestamp(cluster)).isNotEqualTo(actionTimestamp)
    }
}