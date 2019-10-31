package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
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
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class CancelJobTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster("test", "flink")
    private val context = mock(OperatorContext::class.java)
    private val controller = mock(OperatorController::class.java)
    private val resources = mock(OperatorResources::class.java)
    private val time = System.currentTimeMillis()
    private val task = CancelJob()

    @BeforeEach
    fun configure() {
        given(context.lastUpdated).thenReturn(time)
        given(context.controller).thenReturn(controller)
        given(context.resources).thenReturn(resources)
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
    }

    @Test
    fun `onExecuting should return expected result when job is not defined`() {
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        cluster.spec.flinkJob = null
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should return expected result when operation times out`() {
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.currentTimeMillis()).thenReturn(time + OperatorTimeouts.CANCELLING_JOBS_TIMEOUT + 1)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should return expected result when job has been stopped already`() {
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should return expected result when savepoint request can't be created`() {
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.cancelJob(eq(clusterId), any())).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verify(controller, atLeastOnce()).cancelJob(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should return expected result when savepoint request has been created`() {
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.cancelJob(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, SavepointRequest(jobId = "1", triggerId = "100")))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verify(controller, atLeastOnce()).cancelJob(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should set savepoint request when savepoint request has been created`() {
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.cancelJob(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, savepointRequest))
        val result = task.onExecuting(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(OperatorState.getSavepointRequest(cluster)).isEqualTo(savepointRequest)
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when operation times out`() {
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.currentTimeMillis()).thenReturn(time + OperatorTimeouts.CANCELLING_JOBS_TIMEOUT + 1)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint request is missing`() {
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint has not been created yet`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        OperatorState.setSavepointRequest(cluster, savepointRequest)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.AWAIT, ""))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).getSavepointStatus(eq(clusterId), eq(savepointRequest))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint has been created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        OperatorState.setSavepointRequest(cluster, savepointRequest)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.SUCCESS, "/tmp/000"))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).getSavepointStatus(eq(clusterId), eq(savepointRequest))
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should set savepoint path when savepoint has been created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        OperatorState.setSavepointRequest(cluster, savepointRequest)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.SUCCESS, "/tmp/000"))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(OperatorState.getSavepointPath(cluster)).isEqualTo("/tmp/000")
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint can't be created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        OperatorState.setSavepointRequest(cluster, savepointRequest)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.FAILED, ""))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).getSavepointStatus(eq(clusterId), eq(savepointRequest))
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
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