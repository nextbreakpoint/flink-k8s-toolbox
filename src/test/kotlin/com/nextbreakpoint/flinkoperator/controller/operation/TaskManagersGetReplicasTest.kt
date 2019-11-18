package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
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

class TaskManagersGetReplicasTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink", taskManagers = 4)
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val operatorCache = mock(OperatorCache::class.java)
    private val command = TaskManagersGetReplicas(flinkOptions, flinkContext, kubernetesContext)

    @BeforeEach
    fun configure() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorState.setTaskStatus(cluster, TaskStatus.Idle)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.ClusterHalted))
        given(operatorCache.getFlinkCluster(eq(clusterId))).thenReturn(cluster)
        given(kubernetesContext.getTaskManagerStatefulSetReplicas(eq(clusterId))).thenReturn(4)
    }

    @Test
    fun `should fail when cluster doesn't exist`() {
        given(kubernetesContext.getTaskManagerStatefulSetReplicas(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).getTaskManagerStatefulSetReplicas(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isEqualTo(0)
    }

    @Test
    fun `should return expected result`() {
        val actionTimestamp = OperatorAnnotations.getActionTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).getTaskManagerStatefulSetReplicas(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isEqualTo(4)
        assertThat(OperatorAnnotations.getActionTimestamp(cluster)).isEqualTo(actionTimestamp)
    }
}