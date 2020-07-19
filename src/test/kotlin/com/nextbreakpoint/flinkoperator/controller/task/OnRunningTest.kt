package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
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

class OnRunningTest {
    private val savepointOptions = SavepointOptions(targetPath = "file:///tmp")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val logger = mock(Logger::class.java)
    private val context = mock(TaskContext::class.java)
    private val task = OnRunning(logger)

    @BeforeEach
    fun configure() {
        given(context.clusterSelector).thenReturn(clusterSelector)
        given(context.hasBeenDeleted()).thenReturn(false)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(10)
        given(context.timeSinceLastSavepointRequestInSeconds()).thenReturn(70)
        given(context.isDeleteResources()).thenReturn(false)
        given(context.isJobFinished(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = false))
        given(context.isJobFailed(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = false))
        given(context.triggerSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.OK, output = savepointRequest))
        given(context.getLatestSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.OK, output = "file:///tmp/1"))
        given(context.deleteBootstrapJob(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        given(context.doesBootstrapJobExists()).thenReturn(false)
        given(context.doesJobManagerServiceExists()).thenReturn(true)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(true)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(true)
        given(context.computeChanges()).thenReturn(listOf())
        given(context.getSavepointRequest()).thenReturn(null)
        given(context.getSavepointMode()).thenReturn("Manual")
        given(context.getSavepointOtions()).thenReturn(savepointOptions)
        given(context.getSavepointInterval()).thenReturn(60)
        given(context.getManualAction()).thenReturn(ManualAction.NONE)
        given(context.getDesiredTaskManagers()).thenReturn(2)
        given(context.getTaskManagers()).thenReturn(2)
    }

    @Test
    fun `should do nothing when cluster configuration didn't change and job is running`() {
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).getDesiredTaskManagers()
        verify(context, times(1)).getTaskManagers()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to scaling when cluster scale changed`() {
        given(context.getDesiredTaskManagers()).thenReturn(1)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).getDesiredTaskManagers()
        verify(context, times(1)).getTaskManagers()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Scaling)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to updating when cluster configuration changed`() {
        given(context.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Updating)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to stopping when job is failed`() {
        given(context.isJobFailed(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        task.execute(context)
        verify(logger, times(1)).warn(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).setDeleteResources(ArgumentMatchers.eq(false))
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Failed)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to stopping when job is finished`() {
        given(context.isJobFinished(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).setDeleteResources(ArgumentMatchers.eq(false))
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Finished)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when jobmanager service is missing`() {
        given(context.doesJobManagerServiceExists()).thenReturn(false)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when jobmanager statefulset is missing`() {
        given(context.doesJobManagerStatefulSetExists()).thenReturn(false)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when taskmanager statefulset is missing`() {
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(false)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete bootstrap job if job exists`() {
        given(context.doesBootstrapJobExists()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).deleteBootstrapJob(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to cancelling if cluster has been deleted`() {
        given(context.hasBeenDeleted()).thenReturn(true)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).setDeleteResources(ArgumentMatchers.eq(true))
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(eq(ClusterStatus.Cancelling))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should reset manual action when manual action is start`() {
        given(context.getManualAction()).thenReturn(ManualAction.START)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should blank savepoint path when manual action is forget savepoint`() {
        given(context.getManualAction()).thenReturn(ManualAction.FORGET_SAVEPOINT)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setSavepointPath(eq(""))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should create savepoint request when manual action is trigger savepoint`() {
        given(context.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).getSavepointOtions()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setSavepointRequest(eq(savepointRequest))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should do nothing when manual action is trigger savepoint and trigger savepoint fails`() {
        given(context.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        given(context.triggerSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.ERROR, output = savepointRequest))
        task.execute(context)
        verify(logger, times(1)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).getSavepointOtions()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
        verify(context, times(1)).resetManualAction()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to cancelling when manual action is stop`() {
        given(context.getManualAction()).thenReturn(ManualAction.STOP)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Cancelling)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should trigger savepoint when savepoint mode is automatic and enough time passed since last savepoint request`() {
        given(context.getSavepointMode()).thenReturn("Automatic")
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).getSavepointInterval()
        verify(context, times(1)).getSavepointOtions()
        verify(context, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(context, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setSavepointRequest(eq(savepointRequest))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should do nothing when savepoint mode is automatic and not enough time passed since last savepoint request`() {
        given(context.getSavepointMode()).thenReturn("Automatic")
        given(context.timeSinceLastSavepointRequestInSeconds()).thenReturn(20)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).getSavepointInterval()
        verify(context, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).getDesiredTaskManagers()
        verify(context, times(1)).getTaskManagers()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should do nothing when savepoint mode is automatic and trigger savepoint fails`() {
        given(context.getSavepointMode()).thenReturn("Automatic")
        given(context.triggerSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.ERROR, output = savepointRequest))
        task.execute(context)
        verify(logger, times(1)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getSavepointMode()
        verify(context, times(1)).getSavepointInterval()
        verify(context, times(1)).getSavepointOtions()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(context, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
        verify(context, times(1)).getDesiredTaskManagers()
        verify(context, times(1)).getTaskManagers()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should update savepoint path when savepoint request is completed`() {
        given(context.getSavepointRequest()).thenReturn(savepointRequest)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getLatestSavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setSavepointPath(eq("file:///tmp/1"))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should not update savepoint path when savepoint request is not completed`() {
        given(context.getSavepointRequest()).thenReturn(savepointRequest)
        given(context.getLatestSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.ERROR, output = ""))
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getLatestSavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(context, times(1)).getDesiredTaskManagers()
        verify(context, times(1)).getTaskManagers()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should reset savepoint request if savepoint request is not completed after timeout`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(301)
        given(context.getSavepointRequest()).thenReturn(savepointRequest)
        given(context.getLatestSavepoint(any(), any())).thenReturn(OperationResult(status = OperationStatus.ERROR, output = ""))
        task.execute(context)
        verify(logger, times(1)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobFinished(eq(clusterSelector))
        verify(context, times(1)).isJobFailed(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).getSavepointRequest()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getLatestSavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(context, times(1)).resetSavepointRequest()
        verifyNoMoreInteractions(context)
    }
}