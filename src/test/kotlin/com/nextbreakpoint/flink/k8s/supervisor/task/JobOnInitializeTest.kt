package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class JobOnInitializeTest {
    private val context = mock(JobManager::class.java)
    private val task = JobOnInitialise()

    @Test
    fun `should add finalizer`() {
        given(context.hasFinalizer()).thenReturn(false)
        given(context.isResourceDeleted()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).hasFinalizer()
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).addFinalizer()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should terminated`() {
        given(context.hasFinalizer()).thenReturn(false)
        given(context.isResourceDeleted()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).hasFinalizer()
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).onJobTerminated()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should initialize`() {
        given(context.hasFinalizer()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).hasFinalizer()
        inOrder.verify(context, times(1)).onResourceInitialise()
        verifyNoMoreInteractions(context)
    }
}