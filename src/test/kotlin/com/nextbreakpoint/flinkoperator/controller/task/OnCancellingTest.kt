package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class OnCancellingTest {
    private val context = mock(TaskContext::class.java)
    private val task = OnCancelling()

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.hasTaskTimedOut()).thenReturn(false)
        given(context.resetCluster()).thenReturn(true)
        given(context.cancelJob()).thenReturn(false)
    }

    @Test
    fun `should behave as expected when nothing happens`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).cancelJob()
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
    fun `should behave as expected when task is taking too long`() {
        given(context.hasTaskTimedOut()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).onTaskTimeOut()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when bootstrap job is present`() {
        given(context.resetCluster()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).resetCluster()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job has been cancelled`() {
        given(context.cancelJob()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).resetCluster()
        inOrder.verify(context, times(1)).cancelJob()
        inOrder.verify(context, times(1)).onClusterReadyToStop()
        verifyNoMoreInteractions(context)
    }
}