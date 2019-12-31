package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskAction
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
    private val task = CancelJob()

    @BeforeEach
    fun configure() {
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(0)
    }

    @Test
    fun `onExecuting should return expected result when operation times out`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.CANCELLING_JOB_TIMEOUT + 1)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.FAIL)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should return expected result when job has been stopped already`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isJobStopped(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.SKIP)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should return expected result when savepoint request can't be created`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isJobStopped(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        given(context.cancelJob(eq(clusterId), any())).thenReturn(OperationResult(OperationStatus.FAILED, SavepointRequest("", "")))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, atLeastOnce()).isJobStopped(eq(clusterId))
        verify(context, atLeastOnce()).cancelJob(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.REPEAT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should return expected result when savepoint request has been created`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isJobStopped(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        given(context.cancelJob(eq(clusterId), any())).thenReturn(OperationResult(OperationStatus.COMPLETED, SavepointRequest(jobId = "1", triggerId = "100")))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, atLeastOnce()).isJobStopped(eq(clusterId))
        verify(context, atLeastOnce()).cancelJob(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onExecuting should set savepoint request when savepoint request has been created`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        given(context.isJobStopped(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        given(context.cancelJob(eq(clusterId), any())).thenReturn(OperationResult(OperationStatus.COMPLETED, savepointRequest))
        val result = task.onExecuting(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(Status.getSavepointRequest(cluster)).isEqualTo(savepointRequest)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when operation times out`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.CANCELLING_JOB_TIMEOUT + 1)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.FAIL)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint request is missing`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.FAIL)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint has not been created yet`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        Status.setSavepointRequest(cluster, savepointRequest)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.getLatestSavepoint(eq(clusterId), eq(savepointRequest))).thenReturn(OperationResult(OperationStatus.RETRY, ""))
        given(context.isJobStopped(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, atLeastOnce()).getLatestSavepoint(eq(clusterId), eq(savepointRequest))
        verify(context, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint has been created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        Status.setSavepointRequest(cluster, savepointRequest)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.getLatestSavepoint(eq(clusterId), eq(savepointRequest))).thenReturn(OperationResult(OperationStatus.COMPLETED, "/tmp/000"))
        given(context.isJobStopped(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, atLeastOnce()).getLatestSavepoint(eq(clusterId), eq(savepointRequest))
        verify(context, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should set savepoint path when savepoint has been created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        Status.setSavepointRequest(cluster, savepointRequest)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.getLatestSavepoint(eq(clusterId), eq(savepointRequest))).thenReturn(OperationResult(OperationStatus.COMPLETED, "/tmp/000"))
        given(context.isJobStopped(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        val result = task.onAwaiting(context)
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotBlank()
        assertThat(Status.getSavepointPath(cluster)).isEqualTo("/tmp/000")
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onAwaiting should return expected result when savepoint can't be created`() {
        val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
        Status.setSavepointRequest(cluster, savepointRequest)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.getLatestSavepoint(eq(clusterId), eq(savepointRequest))).thenReturn(OperationResult(OperationStatus.FAILED, ""))
        given(context.isJobStopped(eq(clusterId))).thenReturn(OperationResult(OperationStatus.FAILED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, atLeastOnce()).isJobStopped(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.REPEAT)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should return expected result`() {
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onFailed should return expected result`() {
        val result = task.onFailed(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.REPEAT)
        assertThat(result.output).isNotNull()
    }
}