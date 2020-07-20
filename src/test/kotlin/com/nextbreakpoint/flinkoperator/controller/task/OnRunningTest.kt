package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times

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
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).hasScaleChanged()
        inOrder.verify(context, times(1)).updateSavepoint()
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should behave as expected when resource has been deleted`() {
        given(context.isResourceDeleted()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).onResourceDeleted()
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should behave as expected when bootstrap job is present`() {
        given(context.resetCluster()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verifyNoMoreInteractions()
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
        inOrder.verifyNoMoreInteractions()
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
        inOrder.verifyNoMoreInteractions()
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
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should behave as expected when manual action is present and bootstrap is defined`() {
        given(context.isManualActionPresent()).thenReturn(true)
        given(context.isBootstrapPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasJobFinished()
        inOrder.verify(context, times(1)).hasJobFailed()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).isBootstrapPresent()
        inOrder.verify(context, times(1)).executeManualAction(actions, true)
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should behave as expected when manual action is present but bootstrap is not defined`() {
        given(context.isManualActionPresent()).thenReturn(true)
        given(context.isBootstrapPresent()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasJobFinished()
        inOrder.verify(context, times(1)).hasJobFailed()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).isBootstrapPresent()
        inOrder.verify(context, times(1)).executeManualAction(actionsNoTrigger, true)
        inOrder.verifyNoMoreInteractions()
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
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).onResourceChanged()
        inOrder.verifyNoMoreInteractions()
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
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).hasScaleChanged()
        inOrder.verify(context, times(1)).onResourceScaled()
        inOrder.verifyNoMoreInteractions()
    }
}