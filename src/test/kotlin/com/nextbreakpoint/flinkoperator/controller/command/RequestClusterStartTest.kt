package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
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

class RequestClusterStartTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val operatorCache = mock(OperatorCache::class.java)
    private val command = RequestClusterStart(flinkOptions, flinkContext, kubernetesContext, operatorCache)

    @BeforeEach
    fun configure() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.RUNNING)
        OperatorState.setTaskStatus(cluster, TaskStatus.IDLE)
        OperatorState.appendTasks(cluster, listOf(OperatorTask.CLUSTER_HALTED))
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
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when starting without savepoint`() {
        OperatorState.setTaskStatus(cluster, TaskStatus.AWAITING)
        OperatorState.setClusterStatus(cluster, ClusterStatus.TERMINATED)
        val actionTimestamp = OperatorAnnotations.getActionTimestamp(cluster)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verify(kubernetesContext, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(OperatorAnnotations.getManualAction(cluster)).isEqualTo(ManualAction.START)
        assertThat(OperatorAnnotations.isWithSavepoint(cluster)).isEqualTo(true)
        assertThat(OperatorAnnotations.getActionTimestamp(cluster)).isNotEqualTo(actionTimestamp)
    }

    @Test
    fun `should return expected result when starting with savepoint`() {
        OperatorState.setTaskStatus(cluster, TaskStatus.AWAITING)
        OperatorState.setClusterStatus(cluster, ClusterStatus.TERMINATED)
        val actionTimestamp = OperatorAnnotations.getActionTimestamp(cluster)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verify(kubernetesContext, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(OperatorAnnotations.getManualAction(cluster)).isEqualTo(ManualAction.START)
        assertThat(OperatorAnnotations.isWithSavepoint(cluster)).isEqualTo(false)
        assertThat(OperatorAnnotations.getActionTimestamp(cluster)).isNotEqualTo(actionTimestamp)
    }
}