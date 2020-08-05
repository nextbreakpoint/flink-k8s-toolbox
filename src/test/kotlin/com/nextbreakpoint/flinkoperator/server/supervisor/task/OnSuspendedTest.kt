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

class OnSuspendedTest {
    private val context = mock(TaskContext::class.java)
    private val task = OnSuspended()
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
    }

    @Test
    fun `should behave as expected when nothing happens`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).suspendCluster()
        inOrder.verify(context, times(1)).isManualActionPresent()
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
    fun `should behave as expected when cluster is not suspended`() {
        given(context.suspendCluster()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).suspendCluster()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when manual action is present`() {
        given(context.isManualActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).suspendCluster()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).executeManualAction(actions)
        verifyNoMoreInteractions(context)
    }
}