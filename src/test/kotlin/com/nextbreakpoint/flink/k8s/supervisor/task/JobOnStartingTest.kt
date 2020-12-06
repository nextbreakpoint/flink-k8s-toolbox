package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class JobOnStartingTest {
    private val context = mock(JobManager::class.java)
    private val task = JobOnStarting()

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.isClusterStopping()).thenReturn(false)
        given(context.isClusterStopped()).thenReturn(false)
        given(context.isClusterStarting()).thenReturn(false)
        given(context.isClusterStarted()).thenReturn(true)
        given(context.isActionPresent()).thenReturn(false)
        given(context.isClusterUnhealthy()).thenReturn(false)
        given(context.isClusterUpdated()).thenReturn(true)
        given(context.isClusterReady()).thenReturn(true)
        given(context.startJob()).thenReturn(true)
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
    fun `should behave as expected when cluster is stopping`() {
        given(context.isClusterStopping()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).onClusterStopping()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is stopped`() {
        given(context.isClusterStopped()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).onJobAborted()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is starting`() {
        given(context.isClusterStarting()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when manual action is present`() {
        given(context.isActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).executeAction(setOf(Action.STOP))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is unhealthy`() {
        given(context.isClusterUnhealthy()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("UNHEALTHY"))
        inOrder.verify(context, times(1)).onClusterUnhealthy()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster has not started`() {
        given(context.isClusterStarted()).thenReturn(false)
        given(context.isClusterUnhealthy()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("UNHEALTHY"))
        inOrder.verify(context, times(1)).onClusterUnhealthy()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is not updated`() {
        given(context.isClusterUpdated()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).isClusterUpdated()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(false))
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).startJob()
        inOrder.verify(context, times(1)).onJobStarted()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster has timed out`() {
        given(context.hasTaskTimedOut()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).isClusterUpdated()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(true))
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).onJobAborted()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster hasn't started`() {
        given(context.startJob()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).isClusterUpdated()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(true))
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).startJob()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster has started`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).isClusterUpdated()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(true))
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).startJob()
        inOrder.verify(context, times(1)).onJobStarted()
        verifyNoMoreInteractions(context)
    }
}