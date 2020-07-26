package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class OnUpdatingTest {
    private val context = mock(TaskContext::class.java)
    private val task = OnUpdating()

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.hasTaskTimedOut()).thenReturn(false)
        given(context.mustRecreateResources()).thenReturn(true)
        given(context.terminateCluster()).thenReturn(false)
    }

    @Test
    fun `should behave as expected when nothing happens`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).mustRecreateResources()
        inOrder.verify(context, times(1)).terminateCluster()
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
    fun `should behave as expected when mustn't recreate resources`() {
        given(context.mustRecreateResources()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).mustRecreateResources()
        inOrder.verify(context, times(1)).onClusterReadyToUpdate()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when must recreate resources and cluster is terminated`() {
        given(context.mustRecreateResources()).thenReturn(true)
        given(context.terminateCluster()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).mustRecreateResources()
        inOrder.verify(context, times(1)).terminateCluster()
        inOrder.verify(context, times(1)).onClusterReadyToUpdate()
        verifyNoMoreInteractions(context)
    }
}