package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
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

class StopJobTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val context = mock(TaskContext::class.java)
    private val controller = mock(OperationController::class.java)
    private val resources = mock(CachedResources::class.java)
    private val time = System.currentTimeMillis()
    private val task = StopJob()

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
        given(controller.currentTimeMillis()).thenReturn(time + Timeout.STOPPING_JOB_TIMEOUT + 1)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when job has been stopped already`() {
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when job has not been stopped already`() {
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.stopJob(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when job can't be stopped`() {
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.stopJob(eq(clusterId))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when job has been stopped`() {
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.stopJob(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when operation times out`() {
        given(controller.currentTimeMillis()).thenReturn(time + Timeout.STOPPING_JOB_TIMEOUT + 1)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when job has been stopped`() {
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when job has not been stopped yet`() {
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when job can't be stopped`() {
        given(controller.isJobStopped(eq(clusterId))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
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