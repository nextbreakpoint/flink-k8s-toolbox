package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times

class OnFailedTest {
    private val context = mock(TaskContext::class.java)
    private val task = OnFailed()
    private val actions = setOf(
        ManualAction.START,
        ManualAction.STOP,
        ManualAction.FORGET_SAVEPOINT
    )

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.suspendCluster()).thenReturn(true)
        given(context.isManualActionPresent()).thenReturn(false)
        given(context.shouldRestartJob()).thenReturn(false)
    }

    @Test
    fun `should behave as expected when nothing happens`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).suspendCluster()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).shouldRestartJob()
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
    fun `should behave as expected when cluster has not been suspended`() {
        given(context.suspendCluster()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).suspendCluster()
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should behave as expected when manual action is present`() {
        given(context.isManualActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).executeManualAction(eq(actions))
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should behave as expected when job should restart but resource haven't changed`() {
        given(context.shouldRestartJob()).thenReturn(true)
        given(context.hasResourceChanged()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).shouldRestartJob()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should behave as expected when job should restart and resource has changed`() {
        given(context.shouldRestartJob()).thenReturn(true)
        given(context.hasResourceChanged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).shouldRestartJob()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).onResourceChanged()
        inOrder.verifyNoMoreInteractions()
    }
}