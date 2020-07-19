package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
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

class OnFinishedTest {
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val logger = mock(Logger::class.java)
    private val context = mock(TaskContext::class.java)
    private val task = OnFinished(logger)

    @BeforeEach
    fun configure() {
        given(context.clusterSelector).thenReturn(clusterSelector)
        given(context.hasBeenDeleted()).thenReturn(false)
        given(context.isDeleteResources()).thenReturn(false)
        given(context.getManualAction()).thenReturn(ManualAction.NONE)
        given(context.getJobRestartPolicy()).thenReturn("Never")
        given(context.computeChanges()).thenReturn(listOf())
        given(context.arePodsTerminated(clusterSelector)).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        given(context.doesBootstrapJobExists()).thenReturn(false)
        given(context.doesJobManagerServiceExists()).thenReturn(false)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(context.doesJobManagerPVCExists()).thenReturn(false)
        given(context.doesTaskManagerPVCExists()).thenReturn(false)
        given(context.deleteJobManagerService(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        given(context.deleteBootstrapJob(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        given(context.deleteStatefulSets(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        given(context.deletePersistentVolumeClaims(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
    }

    @Test
    fun `should do nothing when cluster configuration didn't change and job is not running`() {
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).getJobRestartPolicy()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should do nothing when restart policy is always and configuration didn't change`() {
        given(context.getJobRestartPolicy()).thenReturn("Always")
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).getJobRestartPolicy()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to updating when restart policy is always and configuration changed`() {
        given(context.getJobRestartPolicy()).thenReturn("Always")
        given(context.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).getJobRestartPolicy()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Updating)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should reset manual action when action is trigger savepoint`() {
        given(context.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should blank savepoint path when action is forget savepoint`() {
        given(context.getManualAction()).thenReturn(ManualAction.FORGET_SAVEPOINT)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setSavepointPath(eq(""))
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to stopping when action is stop`() {
        given(context.getManualAction()).thenReturn(ManualAction.STOP)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).setDeleteResources(ArgumentMatchers.eq(true))
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Stopping)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when action is start`() {
        given(context.getManualAction()).thenReturn(ManualAction.START)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
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
        verify(context, times(1)).deleteBootstrapJob(eq(clusterSelector))
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to stopping if cluster has been deleted`() {
        given(context.hasBeenDeleted()).thenReturn(true)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).setDeleteResources(ArgumentMatchers.eq(true))
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Stopping)
        verifyNoMoreInteractions(context)
    }
}