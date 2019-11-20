package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
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
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val context = mock(TaskContext::class.java)
    private val controller = mock(OperationController::class.java)
    private val resources = mock(CachedResources::class.java)
    private val time = System.currentTimeMillis()
    private val task = CancelJob()

    @BeforeEach
    fun configure() {
        given(context.operatorTimestamp).thenReturn(time)
        given(context.controller).thenReturn(controller)
        given(context.resources).thenReturn(resources)
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
    }

    @Test
    fun `onExecuting should return expected result when operation times out`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(controller.currentTimeMillis()).thenReturn(time + Timeout.CANCELLING_JOB_TIMEOUT + 1)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should return expected result when job has been stopped already`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(controller.isJobRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).isJobRunning(eq(clusterId))
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should return expected result when savepoint request can't be created`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(controller.isJobRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.cancelJob(eq(clusterId), any())).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).isJobRunning(eq(clusterId))
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verify(controller, atLeastOnce()).cancelJob(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should return expected result when savepoint request has been created`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(controller.isJobRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.cancelJob(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, SavepointRequest(jobId = "1", triggerId = "100")))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).isJobRunning(eq(clusterId))
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verify(controller, atLeastOnce()).cancelJob(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should set savepoint request when savepoint request has been created`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        given(controller.isJobRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.cancelJob(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, savepointRequest))
        val result = task.onExecuting(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(Status.getSavepointRequest(cluster)).isEqualTo(savepointRequest)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when operation times out`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(controller.currentTimeMillis()).thenReturn(time + Timeout.CANCELLING_JOB_TIMEOUT + 1)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint request is missing`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint has not been created yet`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        Status.setSavepointRequest(cluster, savepointRequest)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.AWAIT, ""))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).getSavepointStatus(eq(clusterId), eq(savepointRequest))
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint has been created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        Status.setSavepointRequest(cluster, savepointRequest)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.SUCCESS, "/tmp/000"))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).getSavepointStatus(eq(clusterId), eq(savepointRequest))
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should set savepoint path when savepoint has been created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        Status.setSavepointRequest(cluster, savepointRequest)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.SUCCESS, "/tmp/000"))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(Status.getSavepointPath(cluster)).isEqualTo("/tmp/000")
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint can't be created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        Status.setSavepointRequest(cluster, savepointRequest)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(controller.getSavepointStatus(eq(clusterId), eq(savepointRequest))).thenReturn(Result(ResultStatus.FAILED, ""))
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, atLeastOnce()).currentTimeMillis()
        verify(controller, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
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