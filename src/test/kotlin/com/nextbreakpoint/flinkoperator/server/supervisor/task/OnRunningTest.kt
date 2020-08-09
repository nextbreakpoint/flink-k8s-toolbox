package com.nextbreakpoint.flinkoperator.server.supervisor.task

import com.nextbreakpoint.flinkoperator.common.ManualAction
import com.nextbreakpoint.flinkoperator.server.supervisor.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class OnRunningTest {
    private val context = mock(TaskContext::class.java)
    private val task = OnRunning()
    private val actions = setOf(
        ManualAction.START,
        ManualAction.STOP,
        ManualAction.FORGET_SAVEPOINT,
        ManualAction.TRIGGER_SAVEPOINT
    )
    private val actionsNoTrigger = setOf(
        ManualAction.START,
        ManualAction.STOP,
        ManualAction.FORGET_SAVEPOINT
    )

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.resetCluster()).thenReturn(true)
        given(context.hasResourceDiverged()).thenReturn(false)
        given(context.hasJobFinished()).thenReturn(false)
        given(context.hasJobFailed()).thenReturn(false)
        given(context.isManualActionPresent()).thenReturn(false)
        given(context.hasResourceChanged()).thenReturn(false)
        given(context.hasScaleChanged()).thenReturn(false)
    }

    @Test
    fun `should behave as expected when nothing happens`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasJobFinished()
        inOrder.verify(context, times(1)).hasJobFailed()
        inOrder.verify(context, times(1)).hasJobStopped()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).hasScaleChanged()
        inOrder.verify(context, times(1)).updateSavepoint()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resource has been deleted`() {
        given(context.isResourceDeleted()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).onResourceDeleted()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when bootstrap job is present`() {
        given(context.resetCluster()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resource has diverged`() {
        given(context.hasResourceDiverged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).onResourceDiverged()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job has finished`() {
        given(context.hasJobFinished()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasJobFinished()
        inOrder.verify(context, times(1)).onJobFinished()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job has failed`() {
        given(context.hasJobFailed()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasJobFinished()
        inOrder.verify(context, times(1)).hasJobFailed()
        inOrder.verify(context, times(1)).onJobFailed()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job has stopped`() {
        given(context.hasJobStopped()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasJobFinished()
        inOrder.verify(context, times(1)).hasJobFailed()
        inOrder.verify(context, times(1)).hasJobStopped()
        inOrder.verify(context, times(1)).onJobStopped()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when manual action is present`() {
        given(context.isManualActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasJobFinished()
        inOrder.verify(context, times(1)).hasJobFailed()
        inOrder.verify(context, times(1)).hasJobStopped()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).executeManualAction(actions, true)
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resource has changed`() {
        given(context.hasResourceChanged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasJobFinished()
        inOrder.verify(context, times(1)).hasJobFailed()
        inOrder.verify(context, times(1)).hasJobStopped()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).onResourceChanged()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resource has been scaled`() {
        given(context.hasScaleChanged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasJobFinished()
        inOrder.verify(context, times(1)).hasJobFailed()
        inOrder.verify(context, times(1)).hasJobStopped()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).hasScaleChanged()
        inOrder.verify(context, times(1)).onResourceScaled()
        verifyNoMoreInteractions(context)
    }
}