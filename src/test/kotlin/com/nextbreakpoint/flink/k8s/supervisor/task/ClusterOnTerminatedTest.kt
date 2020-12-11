package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterOnTerminatedTest {
    private val context = mock(ClusterManager::class.java)
    private val task = ClusterOnTerminated()

    @Test
    fun `should remove finalizer if not removed`() {
        given(context.hasFinalizer()).thenReturn(true)
        task.execute(context)
        verify(context, times(1)).hasJobFinalizers()
        verify(context, times(1)).hasFinalizer()
        verify(context, times(1)).removeFinalizer()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should not remove finalizer if already removed`() {
        given(context.hasFinalizer()).thenReturn(false)
        task.execute(context)
        verify(context, times(1)).hasJobFinalizers()
        verify(context, times(1)).hasFinalizer()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should not remove finalizer if there are job finalizers`() {
        given(context.hasJobFinalizers()).thenReturn(true)
        task.execute(context)
        verify(context, times(1)).hasJobFinalizers()
        verifyNoMoreInteractions(context)
    }
}