package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterOnStoppingTest {
    private val context = mock(ClusterManager::class.java)
    private val task = ClusterOnStopping()

    @BeforeEach
    fun configure() {
        given(context.waitForJobs()).thenReturn(true)
        given(context.isActionPresent()).thenReturn(false)
        given(context.stopCluster()).thenReturn(true)
        given(context.mustRecreateResources()).thenReturn(false)
        given(context.shouldRestart()).thenReturn(false)
        given(context.isActionPresent()).thenReturn(false)
    }

    @Test
    fun `should behave as expected when jobs haven't been stopped`() {
        given(context.waitForJobs()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).waitForJobs()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster haven't been stopped yet`() {
        given(context.stopCluster()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).waitForJobs()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).stopCluster()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resources must be terminate`() {
        given(context.mustTerminateResources()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).waitForJobs()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).stopCluster()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).onClusterTerminated()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resources don't have to be terminate and cluster should restart`() {
        given(context.mustTerminateResources()).thenReturn(false)
        given(context.shouldRestart()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).waitForJobs()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).stopCluster()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).shouldRestart()
        inOrder.verify(context, times(1)).onClusterReadyToRestart()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resources don't have to be terminate and there is an action`() {
        given(context.mustTerminateResources()).thenReturn(false)
        given(context.isActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).waitForJobs()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).stopCluster()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).shouldRestart()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).executeAction(eq(setOf(Action.START)))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resources don't have to be terminate and cluster has stopped`() {
        given(context.mustTerminateResources()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).waitForJobs()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).stopCluster()
        inOrder.verify(context, times(1)).mustTerminateResources()
        inOrder.verify(context, times(1)).shouldRestart()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).onClusterStopped()
        verifyNoMoreInteractions(context)
    }
}