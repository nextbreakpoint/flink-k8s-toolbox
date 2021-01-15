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

class ClusterOnStartingTest {
    private val context = mock(ClusterManager::class.java)
    private val task = ClusterOnStarting()

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.isActionPresent()).thenReturn(false)
        given(context.ensureJobManagerPodExists()).thenReturn(true)
        given(context.ensureJobManagerServiceExists()).thenReturn(true)
        given(context.isClusterHealthy()).thenReturn(true)
        given(context.stopAllJobs()).thenReturn(true)
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
    fun `should behave as expected when manual action is present`() {
        given(context.isActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).executeAction(eq(setOf(Action.STOP)))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when specification has changed`() {
        given(context.hasSpecificationChanged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).onResourceChanged()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when jobmanager pod doesn't exit`() {
        given(context.ensureJobManagerPodExists()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).ensureJobManagerPodExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when jobmanager service doesn't exit`() {
        given(context.ensureJobManagerServiceExists()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).ensureJobManagerPodExists()
        inOrder.verify(context, times(1)).ensureJobManagerServiceExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is not healthy`() {
        given(context.isClusterHealthy()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).ensureJobManagerPodExists()
        inOrder.verify(context, times(1)).ensureJobManagerServiceExists()
        inOrder.verify(context, times(1)).isClusterHealthy()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when jobs haven't been stopped`() {
        given(context.stopAllJobs()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).ensureJobManagerPodExists()
        inOrder.verify(context, times(1)).ensureJobManagerServiceExists()
        inOrder.verify(context, times(1)).isClusterHealthy()
        inOrder.verify(context, times(1)).stopAllJobs()
        verifyNoMoreInteractions(context)
    }

//    @Test
//    fun `should behave as expected when jobs haven't been created`() {
//        given(context.createJobs()).thenReturn(false)
//        task.execute(context)
//        val inOrder = inOrder(context)
//        inOrder.verify(context, times(1)).isResourceDeleted()
//        inOrder.verify(context, times(1)).isActionPresent()
//        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
//        inOrder.verify(context, times(1)).ensureJobManagerPodExists()
//        inOrder.verify(context, times(1)).ensureJobManagerServiceExists()
//        inOrder.verify(context, times(1)).isClusterHealthy()
//        inOrder.verify(context, times(1)).stopAllJobs()
////        inOrder.verify(context, times(1)).createJobs()
//        verifyNoMoreInteractions(context)
//    }

    @Test
    fun `should behave as expected when cluster has started`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).ensureJobManagerPodExists()
        inOrder.verify(context, times(1)).ensureJobManagerServiceExists()
        inOrder.verify(context, times(1)).isClusterHealthy()
        inOrder.verify(context, times(1)).stopAllJobs()
//        inOrder.verify(context, times(1)).createJobs()
        inOrder.verify(context, times(1)).onClusterStarted()
        verifyNoMoreInteractions(context)
    }
}