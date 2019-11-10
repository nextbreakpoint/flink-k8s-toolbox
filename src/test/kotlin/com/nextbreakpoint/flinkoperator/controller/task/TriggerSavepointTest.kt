package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorTimeouts
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class TriggerSavepointTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val context = mock(OperatorContext::class.java)
    private val controller = mock(OperatorController::class.java)
    private val resources = mock(OperatorResources::class.java)
    private val time = System.currentTimeMillis()
    private val task = TriggerSavepoint()

    @BeforeEach
    fun configure() {
        given(context.operatorTimestamp).thenReturn(time)
        given(context.controller).thenReturn(controller)
        given(context.resources).thenReturn(resources)
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
    }

    @Test
    fun `onExecuting should return expected result when job is not defined`() {
        cluster.spec.bootstrap = null
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when operation times out`() {
        given(controller.currentTimeMillis()).thenReturn(time + OperatorTimeouts.CREATING_SAVEPOINT_TIMEOUT + 1)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when savepoint request has been created already`() {
        OperatorState.setSavepointRequest(cluster, SavepointRequest(jobId = "1", triggerId = "100"))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when a savepoint is already in progress`() {
        given(controller.triggerSavepoint(eq(clusterId), any())).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).triggerSavepoint(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when savepoint request can't be created`() {
        given(controller.triggerSavepoint(eq(clusterId), any())).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).triggerSavepoint(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when savepoint request has been created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        given(controller.triggerSavepoint(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, savepointRequest))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).triggerSavepoint(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should set savepoint request when savepoint request has been created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        given(controller.triggerSavepoint(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, savepointRequest))
        val result = task.onExecuting(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(OperatorState.getSavepointRequest(cluster)).isEqualTo(savepointRequest)
    }

    @Test
    fun `onAwaiting should return expected result when operation times out`() {
        given(controller.currentTimeMillis()).thenReturn(time + OperatorTimeouts.CREATING_SAVEPOINT_TIMEOUT + 1)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when savepoint request is missing`() {
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when savepoint has not been completed yet`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        OperatorState.setSavepointRequest(cluster, savepointRequest)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.AWAIT, ""))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).getSavepointStatus(eq(clusterId), eq(savepointRequest))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when savepoint has been completed`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        OperatorState.setSavepointRequest(cluster, savepointRequest)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.SUCCESS, "/tmp/000"))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).getSavepointStatus(eq(clusterId), eq(savepointRequest))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should set savepoint path when savepoint has been completed`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        OperatorState.setSavepointRequest(cluster, savepointRequest)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.SUCCESS, "/tmp/000"))
        val result = task.onAwaiting(context)
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(OperatorState.getSavepointPath(cluster)).isEqualTo("/tmp/000")
    }

    @Test
    fun `onAwaiting should return expected result when savepoint has failed`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        OperatorState.setSavepointRequest(cluster, savepointRequest)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.FAILED, ""))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).getSavepointStatus(eq(clusterId), eq(savepointRequest))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onIdle should return expected result`() {
        val result = task.onIdle(context)
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onFailed should return expected result`() {
        val result = task.onFailed(context)
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }
}