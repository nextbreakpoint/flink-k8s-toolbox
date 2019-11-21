package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class RescaleClusterTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink", taskManagers = 4)
    private val context = mock(TaskContext::class.java)
    private val task = RescaleCluster()

    @BeforeEach
    fun configure() {
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(0)
        Status.setTaskManagers(cluster, 4)
        Status.setTaskSlots(cluster, 1)
    }

    @Test
    fun `onExecuting should return expected result when operation times out`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.RESCALING_CLUSTER_TIMEOUT + 1)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when task managers can't be scaled`() {
        given(context.setTaskManagersReplicas(eq(clusterId), Mockito.eq(4))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).setTaskManagersReplicas(eq(clusterId), Mockito.eq(4))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when task managers have been scaled`() {
        given(context.setTaskManagersReplicas(eq(clusterId), Mockito.eq(4))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).setTaskManagersReplicas(eq(clusterId), Mockito.eq(4))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when operation times out`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.RESCALING_CLUSTER_TIMEOUT + 1)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when resources are not ready`() {
        given(context.getTaskManagersReplicas(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, 0))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getTaskManagersReplicas(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when can't get current replicas`() {
        given(context.getTaskManagersReplicas(eq(clusterId))).thenReturn(Result(ResultStatus.FAILED, 0))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getTaskManagersReplicas(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when not all resources are ready`() {
        given(context.getTaskManagersReplicas(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, 2))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getTaskManagersReplicas(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when resources are ready`() {
        given(context.getTaskManagersReplicas(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, 4))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getTaskManagersReplicas(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onIdle should return expected result`() {
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onFailed should return expected result`() {
        val result = task.onFailed(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }
}