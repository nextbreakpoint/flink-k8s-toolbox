package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
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

class OnSuspendedTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val logger = mock(Logger::class.java)
    private val context = mock(TaskContext::class.java)
    private val task = OnSuspended(logger)

    @BeforeEach
    fun configure() {
        given(context.clusterId).thenReturn(clusterId)
        given(context.hasBeenDeleted()).thenReturn(false)
        given(context.doesBootstrapExists()).thenReturn(false)
        given(context.doesJobManagerServiceExists()).thenReturn(false)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(true)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(true)
        given(context.doesJobManagerPVCExists()).thenReturn(true)
        given(context.doesTaskManagerPVCExists()).thenReturn(true)
        given(context.getManualAction()).thenReturn(ManualAction.NONE)
        given(context.computeChanges()).thenReturn(listOf())
        given(context.arePodsTerminated(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.deleteBootstrapJob(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.deleteJobManagerService(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.deleteStatefulSets(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.deletePersistentVolumeClaims(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
    }

    @Test
    fun `should do nothing when cluster configuration didn't change and pods are not running`() {
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).computeChanges()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete jobmanager service if service exists`() {
        given(context.doesJobManagerServiceExists()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).deleteJobManagerService(eq(clusterId))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete bootstrap job if job exists`() {
        given(context.doesBootstrapExists()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).deleteBootstrapJob(eq(clusterId))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should terminate pods if pods are still running`() {
        given(context.arePodsTerminated(any())).thenReturn(OperationResult(status = OperationStatus.RETRY, output = null))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).terminatePods(eq(clusterId))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to terminated if cluster configuration changed and resource have been deleted`() {
        given(context.doesBootstrapExists()).thenReturn(false)
        given(context.doesJobManagerServiceExists()).thenReturn(false)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(context.doesJobManagerPVCExists()).thenReturn(false)
        given(context.doesTaskManagerPVCExists()).thenReturn(false)
        given(context.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(2)).arePodsTerminated(eq(clusterId))
        verify(context, times(2)).doesBootstrapExists()
        verify(context, times(2)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).updateDigests()
        verify(context, times(1)).updateStatus()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Terminated)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete jobmanager and taskmanager PVCs if cluster configuration changed`() {
        given(context.doesBootstrapExists()).thenReturn(false)
        given(context.doesJobManagerServiceExists()).thenReturn(false)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(context.doesJobManagerPVCExists()).thenReturn(true)
        given(context.doesTaskManagerPVCExists()).thenReturn(true)
        given(context.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(2)).arePodsTerminated(eq(clusterId))
        verify(context, times(2)).doesBootstrapExists()
        verify(context, times(2)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).deletePersistentVolumeClaims(eq(clusterId))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete jobmanager and taskmanager statefulsets if cluster configuration changed`() {
        given(context.doesBootstrapExists()).thenReturn(false)
        given(context.doesJobManagerServiceExists()).thenReturn(false)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(true)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(true)
        given(context.doesJobManagerPVCExists()).thenReturn(false)
        given(context.doesTaskManagerPVCExists()).thenReturn(false)
        given(context.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(2)).arePodsTerminated(eq(clusterId))
        verify(context, times(2)).doesBootstrapExists()
        verify(context, times(2)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).deleteStatefulSets(eq(clusterId))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete jobmanager service if cluster configuration changed`() {
        given(context.doesBootstrapExists()).thenReturn(false)
        given(context.doesJobManagerServiceExists()).thenReturn(true)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(context.doesJobManagerPVCExists()).thenReturn(false)
        given(context.doesTaskManagerPVCExists()).thenReturn(false)
        given(context.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
//        verify(context, times(1)).doesJobManagerStatefulSetExists()
//        verify(context, times(1)).doesTaskManagerStatefulSetExists()
//        verify(context, times(1)).doesJobManagerPVCExists()
//        verify(context, times(1)).doesTaskManagerPVCExists()
//        verify(context, times(1)).computeChanges()
        verify(context, times(1)).deleteJobManagerService(eq(clusterId))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete bootstrap job if cluster configuration changed`() {
        given(context.doesBootstrapExists()).thenReturn(true)
        given(context.doesJobManagerServiceExists()).thenReturn(false)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(context.doesJobManagerPVCExists()).thenReturn(false)
        given(context.doesTaskManagerPVCExists()).thenReturn(false)
        given(context.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
//        verify(context, times(1)).doesJobManagerStatefulSetExists()
//        verify(context, times(1)).doesTaskManagerStatefulSetExists()
//        verify(context, times(1)).doesJobManagerPVCExists()
//        verify(context, times(1)).doesTaskManagerPVCExists()
//        verify(context, times(1)).computeChanges()
        verify(context, times(1)).deleteBootstrapJob(eq(clusterId))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to terminated if resources have been deleted`() {
        given(context.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(false)
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Terminated)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should reset manual action when manual action is trigger savepoint`() {
        given(context.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should blank savepoint path when manual action is forget savepoint`() {
        given(context.getManualAction()).thenReturn(ManualAction.FORGET_SAVEPOINT)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setSavepointPath(eq(""))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when manual action is start`() {
        given(context.getManualAction()).thenReturn(ManualAction.START)
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to stopping when manual action is stop`() {
        given(context.getManualAction()).thenReturn(ManualAction.STOP)
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).arePodsTerminated(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Stopping)
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
        verify(context, times(1)).setClusterStatus(ClusterStatus.Stopping)
        verifyNoMoreInteractions(context)
    }
}