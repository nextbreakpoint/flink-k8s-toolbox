package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterOnStartedTest {
    private val context = mock(ClusterManager::class.java)
    private val task = ClusterOnStarted()

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.hasResourceDiverged()).thenReturn(false)
        given(context.hasSpecificationChanged()).thenReturn(false)
        given(context.isActionPresent()).thenReturn(false)
        given(context.isClusterUnhealthy()).thenReturn(false)
        given(context.areJobsUpdating()).thenReturn(false)
        given(context.stopUnmanagedJobs()).thenReturn(true)
        given(context.rescaleTaskManagers()).thenReturn(false)
        given(context.rescaleTaskManagerPods()).thenReturn(false)
        given(context.isClusterReady()).thenReturn(true)
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
    fun `should behave as expected when resource has diverged`() {
        given(context.hasResourceDiverged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).onResourceDiverged()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resource has changed`() {
        given(context.hasSpecificationChanged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).onResourceChanged()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when manual action is present`() {
        given(context.isActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).executeAction(setOf(ManualAction.STOP))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is unhealthy`() {
        given(context.isClusterUnhealthy()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("UNHEALTHY"))
        inOrder.verify(context, times(1)).onClusterUnhealthy()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when jobs are updating`() {
        given(context.areJobsUpdating()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).rescaleTaskManagers()
        inOrder.verify(context, times(1)).rescaleTaskManagerPods()
        inOrder.verify(context, times(1)).isClusterReady()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(true))
        inOrder.verify(context, times(1)).areJobsUpdating()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when unmanaged jobs haven't been stopped`() {
        given(context.stopUnmanagedJobs()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).rescaleTaskManagers()
        inOrder.verify(context, times(1)).rescaleTaskManagerPods()
        inOrder.verify(context, times(1)).isClusterReady()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(true))
        inOrder.verify(context, times(1)).areJobsUpdating()
        inOrder.verify(context, times(1)).stopUnmanagedJobs()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster has been rescaled`() {
        given(context.rescaleTaskManagers()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).rescaleTaskManagers()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(false))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when taskmanagers pods have been rescaled`() {
        given(context.rescaleTaskManagerPods()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).rescaleTaskManagers()
        inOrder.verify(context, times(1)).rescaleTaskManagerPods()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(false))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is not ready`() {
        given(context.isClusterReady()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).rescaleTaskManagers()
        inOrder.verify(context, times(1)).rescaleTaskManagerPods()
        inOrder.verify(context, times(1)).isClusterReady()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(false))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is ready`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasResourceDiverged()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).rescaleTaskManagers()
        inOrder.verify(context, times(1)).rescaleTaskManagerPods()
        inOrder.verify(context, times(1)).isClusterReady()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(true))
        inOrder.verify(context, times(1)).areJobsUpdating()
        inOrder.verify(context, times(1)).stopUnmanagedJobs()
        verifyNoMoreInteractions(context)
    }
}