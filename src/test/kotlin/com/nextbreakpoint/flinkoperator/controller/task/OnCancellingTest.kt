package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.apache.log4j.Logger
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.atLeast
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class OnCancellingTest {
    private val savepointOptions = SavepointOptions(targetPath = "file:///tmp")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val logger = mock(Logger::class.java)
    private val context = mock(TaskContext::class.java)
    private val task = OnCancelling(logger)

    @BeforeEach
    fun configure() {
        given(context.clusterSelector).thenReturn(clusterSelector)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(10)
        given(context.deleteBootstrapJob(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        given(context.triggerSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.OK, output = savepointRequest))
        given(context.getLatestSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.OK, output = "file:///tmp/1"))
        given(context.stopJob(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        given(context.cancelJob(any(), eq(savepointOptions))).thenReturn(OperationResult(status = OperationStatus.OK, output = savepointRequest))
        given(context.doesBootstrapJobExists()).thenReturn(false)
        given(context.isSavepointRequired()).thenReturn(false)
        given(context.getSavepointRequest()).thenReturn(null)
        given(context.getSavepointOtions()).thenReturn(savepointOptions)
    }

    @Test
    fun `should change status to stopping when savepoint is not required and job has been stopped`() {
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).stopJob(eq(clusterSelector))
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Stopping)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should not change status to stopping when savepoint is not required and job can't be stopped`() {
        given(context.stopJob(any())).thenReturn(OperationResult(status = OperationStatus.ERROR, output = null))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).stopJob(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should not change status to stopping when savepoint is required and job can't be stopped`() {
        given(context.isSavepointRequired()).thenReturn(true)
        given(context.cancelJob(any(), any())).thenReturn(OperationResult(status = OperationStatus.ERROR, output = SavepointRequest(jobId = "", triggerId = "")))
        task.execute(context)
        verify(logger, atLeast(1)).warn(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointOtions()
        verify(context, times(1)).cancelJob(eq(clusterSelector), eq(savepointOptions))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should create savepoint request when savepoint is required and job has been cancelled`() {
        given(context.isSavepointRequired()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointOtions()
        verify(context, times(1)).cancelJob(eq(clusterSelector), eq(savepointOptions))
        verify(context, times(1)).setSavepointRequest(eq(savepointRequest))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should not create savepoint request when savepoint is required and job has not been cancelled`() {
        given(context.cancelJob(any(), eq(savepointOptions))).thenReturn(OperationResult(status = OperationStatus.ERROR, output = null))
        given(context.isSavepointRequired()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).warn(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointOtions()
        verify(context, times(1)).cancelJob(eq(clusterSelector), eq(savepointOptions))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should update savepoint path and change status to stopping when savepoint is completed`() {
        given(context.getSavepointRequest()).thenReturn(savepointRequest)
        given(context.isSavepointRequired()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getLatestSavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setSavepointPath(eq("file:///tmp/1"))
        verify(context, times(1)).setClusterStatus(ClusterStatus.Stopping)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should do nothing when job has not been cancelled yet and savepoint is not completed`() {
        given(context.getLatestSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.ERROR, output = "file:///tmp/1"))
        given(context.getSavepointRequest()).thenReturn(savepointRequest)
        given(context.isSavepointRequired()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getLatestSavepoint(eq(clusterSelector), eq(savepointRequest))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should not change status when savepoint is failed`() {
        given(context.getLatestSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.ERROR, output = "file:///tmp/1"))
        given(context.getSavepointRequest()).thenReturn(savepointRequest)
        given(context.isSavepointRequired()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getLatestSavepoint(eq(clusterSelector), eq(savepointRequest))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete bootstrap job if job exists`() {
        given(context.doesBootstrapJobExists()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).deleteBootstrapJob(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to failed if job is not stopped after timeout`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(301)
        task.execute(context)
        verify(logger, times(1)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Failed)
        verifyNoMoreInteractions(context)
    }
}