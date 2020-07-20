package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times

class OnTerminatedTest {
    private val context = mock(TaskContext::class.java)
    private val task = OnTerminated()
    private val actions = setOf(
        ManualAction.START,
        ManualAction.FORGET_SAVEPOINT
    )

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.terminateCluster()).thenReturn(true)
        given(context.isManualActionPresent()).thenReturn(false)
    }

    @Test
    fun `should behave as expected when nothing happens`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).terminateCluster()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should behave as expected when resource has been deleted`() {
        given(context.isResourceDeleted()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).removeFinalizer()
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should behave as expected when cluster is not terminated`() {
        given(context.terminateCluster()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).terminateCluster()
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should behave as expected when manual action is present`() {
        given(context.isManualActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).terminateCluster()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).executeManualAction(actions)
        inOrder.verifyNoMoreInteractions()
    }
}