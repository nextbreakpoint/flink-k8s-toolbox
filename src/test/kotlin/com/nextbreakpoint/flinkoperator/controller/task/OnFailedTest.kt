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

class OnFailedTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val logger = mock(Logger::class.java)
    private val context = mock(TaskContext::class.java)
    private val task = OnFailed(logger)

    @BeforeEach
    fun configure() {
        given(context.clusterId).thenReturn(clusterId)
        given(context.hasBeenDeleted()).thenReturn(false)
        given(context.isDeleteResources()).thenReturn(false)
        given(context.isBatchMode()).thenReturn(false)
        given(context.isJobRunning(any())).thenReturn(OperationResult(status = OperationStatus.RETRY, output = null))
        given(context.deleteBootstrapJob(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        given(context.doesBootstrapExists()).thenReturn(false)
        given(context.getManualAction()).thenReturn(ManualAction.NONE)
        given(context.getJobRestartPolicy()).thenReturn("Never")
        given(context.computeChanges()).thenReturn(listOf())
    }

    @Test
    fun `should do nothing when cluster configuration didn't change and job is not running`() {
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isBatchMode()
        verify(context, times(1)).isJobRunning(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).getJobRestartPolicy()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should do nothing when restart policy is always and configuration didn't change`() {
        given(context.getJobRestartPolicy()).thenReturn("Always")
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isBatchMode()
        verify(context, times(1)).isJobRunning(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).getJobRestartPolicy()
        verify(context, times(1)).computeChanges()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to updating when restart policy is always and configuration changed`() {
        given(context.getJobRestartPolicy()).thenReturn("Always")
        given(context.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isBatchMode()
        verify(context, times(1)).isJobRunning(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).getJobRestartPolicy()
        verify(context, times(1)).computeChanges()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Updating)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should reset manual action when action is trigger savepoint`() {
        given(context.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobRunning(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should blank savepoint path when action is forget savepoint`() {
        given(context.getManualAction()).thenReturn(ManualAction.FORGET_SAVEPOINT)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobRunning(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setSavepointPath(eq(""))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to stopping when action is stop`() {
        given(context.getManualAction()).thenReturn(ManualAction.STOP)
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobRunning(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).setDeleteResources(ArgumentMatchers.eq(true))
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Stopping)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to starting when action is start`() {
        given(context.getManualAction()).thenReturn(ManualAction.START)
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobRunning(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).getManualAction()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to running when job is running`() {
        given(context.isJobRunning(any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterId
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).isJobRunning(eq(clusterId))
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Running)
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
        verify(context, times(1)).doesBootstrapExists()
        verify(context, times(1)).deleteBootstrapJob(eq(clusterId))
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