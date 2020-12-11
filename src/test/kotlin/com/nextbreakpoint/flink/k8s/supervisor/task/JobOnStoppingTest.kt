package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class JobOnStoppingTest {
    private val context = mock(JobManager::class.java)
    private val task = JobOnStopping()

    @BeforeEach
    fun configure() {
        given(context.terminateBootstrapJob()).thenReturn(true)
        given(context.isClusterStarted()).thenReturn(true)
        given(context.isClusterUnhealthy()).thenReturn(false)
        given(context.cancelJob()).thenReturn(true)
        given(context.mustTerminateResources()).thenReturn(false)
        given(context.shouldRestart()).thenReturn(false)
        given(context.hasFinalizer()).thenReturn(true)
    }

    @Test
    fun `should behave as expected when bootstrap job hasn't been deleted yet`() {
        given(context.terminateBootstrapJob()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is unhealthy and resources don't have to be deleted`() {
        given(context.isClusterUnhealthy()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth("UNHEALTHY")
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).onJobStopped()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is unhealthy and resources must be deleted`() {
        given(context.mustTerminateResources()).thenReturn(true)
        given(context.isClusterUnhealthy()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth("UNHEALTHY")
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).onJobTerminated()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job hasn't been canceled yet`() {
        given(context.cancelJob()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).cancelJob()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job has been canceled and resources don't have to be deleted`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).cancelJob()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).shouldRestart()
        inOrder.verify(context, times(1)).onJobStopped()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job has been canceled and resources must be deleted`() {
        given(context.mustTerminateResources()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).cancelJob()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).onJobTerminated()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job has been canceled and job should restart`() {
        given(context.shouldRestart()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).cancelJob()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).shouldRestart()
        inOrder.verify(context, times(1)).onJobReadyToRestart()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster hasn't started`() {
        given(context.isClusterStarted()).thenReturn(false)
        given(context.shouldRestart()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth("")
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).cancelJob()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).shouldRestart()
        inOrder.verify(context, times(1)).onJobReadyToRestart()
        verifyNoMoreInteractions(context)
    }
}