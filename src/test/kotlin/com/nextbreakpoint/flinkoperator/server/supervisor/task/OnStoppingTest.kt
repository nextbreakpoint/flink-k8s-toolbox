package com.nextbreakpoint.flinkoperator.server.supervisor.task

import com.nextbreakpoint.flinkoperator.server.supervisor.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class OnStoppingTest {
    private val context = mock(TaskContext::class.java)
    private val task = OnStopping()

    @BeforeEach
    fun configure() {
        given(context.hasTaskTimedOut()).thenReturn(false)
        given(context.terminateCluster()).thenReturn(false)
        given(context.suspendCluster()).thenReturn(false)
    }

    @Test
    fun `should behave as expected when task is taking too long`() {
        given(context.hasTaskTimedOut()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).onTaskTimeOut()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when nothing happens and resources mustn't terminate`() {
        given(context.mustTerminateResources()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).suspendCluster()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when nothing happens and resources mustn't terminate and cluster is suspended`() {
        given(context.mustTerminateResources()).thenReturn(false)
        given(context.suspendCluster()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).suspendCluster()
        inOrder.verify(context, times(1)).onClusterSuspended()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when nothing happens and resources must terminated`() {
        given(context.mustTerminateResources()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).terminateCluster()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when nothing happens and resources must terminated and cluster is terminated`() {
        given(context.mustTerminateResources()).thenReturn(true)
        given(context.terminateCluster()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).terminateCluster()
        inOrder.verify(context, times(1)).onClusterTerminated()
        verifyNoMoreInteractions(context)
    }
}