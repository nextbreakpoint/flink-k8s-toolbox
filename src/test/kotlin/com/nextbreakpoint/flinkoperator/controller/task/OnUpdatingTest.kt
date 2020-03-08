package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
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
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.atLeast
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class OnUpdatingTest {
    private val savepointOptions = SavepointOptions(targetPath = "file:///tmp")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val logger = mock(Logger::class.java)
    private val context = mock(TaskContext::class.java)
    private val task = OnUpdating(logger)

    @BeforeEach
    fun configure() {
        given(context.clusterId).thenReturn(clusterId)
        given(context.hasBeenDeleted()).thenReturn(false)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(10)
        given(context.doesBootstrapExists()).thenReturn(false)
        given(context.doesJobManagerServiceExists()).thenReturn(false)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(context.doesJobManagerPVCExists()).thenReturn(false)
        given(context.doesTaskManagerPVCExists()).thenReturn(false)
        given(context.computeChanges()).thenReturn(listOf())
        given(context.getSavepointRequest()).thenReturn(null)
        given(context.getSavepointMode()).thenReturn("Manual")
        given(context.getSavepointOtions()).thenReturn(savepointOptions)
        given(context.getSavepointInterval()).thenReturn(60)
        given(context.isSavepointRequired()).thenReturn(false)
        given(context.arePodsTerminated(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.deleteBootstrapJob(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.deleteJobManagerService(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.deleteStatefulSets(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.deletePersistentVolumeClaims(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.isJobRunning(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.stopJob(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.cancelJob(any(), eq(savepointOptions))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = savepointRequest))
        given(context.triggerSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = savepointRequest))
        given(context.getLatestSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = "file:///tmp/1"))
    }

    @Test
    fun `should change status to starting when cluster configuration didn't change`() {
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when jobmanager configuration has changed and resources have been deleted`() {
        given(context.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).updateDigests()
        verify(context, times(1)).updateStatus()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when taskmanager configuration has changed and resources have been deleted`() {
        given(context.computeChanges()).thenReturn(listOf("TASK_MANAGER"))
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).updateDigests()
        verify(context, times(1)).updateStatus()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when runtime configuration has changed and resources have been deleted`() {
        given(context.computeChanges()).thenReturn(listOf("RUNTIME"))
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).updateDigests()
        verify(context, times(1)).updateStatus()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting and update savepoint path when bootstrap configuration has changed and job has been stopped`() {
        given(context.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).stopJob(eq(clusterId))
        verify(context, times(1)).updateDigests()
        verify(context, times(1)).updateStatus()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting and update savepoint path when bootstrap configuration has changed and job can't be stopped`() {
        given(context.stopJob(any())).thenReturn(OperationResult(status = OperationStatus.FAILED, output = null))
        given(context.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(1)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).stopJob(eq(clusterId))
        verify(context, times(1)).updateDigests()
        verify(context, times(1)).updateStatus()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should create savepoint request when bootstrap configuration has changed and savepoint is required`() {
        given(context.isSavepointRequired()).thenReturn(true)
        given(context.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointOtions()
        verify(context, times(1)).cancelJob(eq(clusterId), eq(savepointOptions))
        verify(context, times(1)).setSavepointRequest(eq(savepointRequest))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should not create savepoint request when bootstrap configuration has changed and savepoint is required but job has not been cancelled`() {
        given(context.cancelJob(any(), eq(savepointOptions))).thenReturn(OperationResult(status = OperationStatus.RETRY, output = savepointRequest))
        given(context.isSavepointRequired()).thenReturn(true)
        given(context.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointOtions()
        verify(context, times(1)).cancelJob(eq(clusterId), eq(savepointOptions))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when bootstrap configuration has changed but job can't be cancelled`() {
        given(context.cancelJob(any(), eq(savepointOptions))).thenReturn(OperationResult(status = OperationStatus.FAILED, output = savepointRequest))
        given(context.isSavepointRequired()).thenReturn(true)
        given(context.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(1)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointOtions()
        verify(context, times(1)).cancelJob(eq(clusterId), eq(savepointOptions))
        verify(context, times(1)).updateDigests()
        verify(context, times(1)).updateStatus()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when bootstrap configuration has changed and savepoint failed`() {
        given(context.getLatestSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.FAILED, output = "file:///tmp/1"))
        given(context.isSavepointRequired()).thenReturn(true)
        given(context.getSavepointRequest()).thenReturn(savepointRequest)
        given(context.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(1)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getLatestSavepoint(eq(clusterId), eq(savepointRequest))
        verify(context, times(1)).updateDigests()
        verify(context, times(1)).updateStatus()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when bootstrap configuration has changed and job has been cancelled`() {
        given(context.isSavepointRequired()).thenReturn(true)
        given(context.getSavepointRequest()).thenReturn(savepointRequest)
        given(context.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getLatestSavepoint(eq(clusterId), eq(savepointRequest))
        verify(context, times(1)).setSavepointPath(eq("file:///tmp/1"))
        verify(context, times(1)).updateDigests()
        verify(context, times(1)).updateStatus()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should do nothing when when bootstrap configuration has changed and job has not been cancelled yet and savepoint is not completed`() {
        given(context.getLatestSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.RETRY, output = "file:///tmp/1"))
        given(context.isSavepointRequired()).thenReturn(true)
        given(context.getSavepointRequest()).thenReturn(savepointRequest)
        given(context.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).isSavepointRequired()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getLatestSavepoint(eq(clusterId), eq(savepointRequest))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete bootstrap job when bootstrap configuration has changed and job exists`() {
        given(context.doesBootstrapExists()).thenReturn(true)
        given(context.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).deleteBootstrapJob(eq(clusterId))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to failed if cluster not updated after timeout`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(301)
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(1)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(eq(ClusterStatus.Failed))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to stopping if cluster has been deleted`() {
        given(context.hasBeenDeleted()).thenReturn(true)
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).setDeleteResources(ArgumentMatchers.eq(true))
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(eq(ClusterStatus.Stopping))
        verifyNoMoreInteractions(context)
    }
}