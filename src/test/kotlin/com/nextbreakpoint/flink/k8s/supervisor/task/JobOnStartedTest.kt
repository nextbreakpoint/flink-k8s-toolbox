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

class JobOnStartedTest {
    private val context = mock(JobManager::class.java)
    private val task = JobOnStarted()

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.isClusterStopping()).thenReturn(false)
        given(context.isClusterStopped()).thenReturn(false)
        given(context.isClusterStarting()).thenReturn(false)
        given(context.isClusterStarted()).thenReturn(true)
        given(context.isClusterTerminated()).thenReturn(false)
        given(context.isClusterUnhealthy()).thenReturn(false)
        given(context.hasSpecificationChanged()).thenReturn(false)
        given(context.hasParallelismChanged()).thenReturn(false)
        given(context.isJobFinished()).thenReturn(false)
        given(context.isJobCancelled()).thenReturn(false)
        given(context.isJobFailed()).thenReturn(false)
        given(context.isActionPresent()).thenReturn(false)
        given(context.updateSavepoint()).thenReturn(true)
        given(context.hasFinalizer()).thenReturn(true)
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
        inOrder.verify(context, times(1)).isClusterTerminated()
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
        inOrder.verify(context, times(1)).isClusterTerminated()
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
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).onJobAborted()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is unhealthy`() {
        given(context.isClusterUnhealthy()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("UNHEALTHY"))
        inOrder.verify(context, times(1)).onClusterUnhealthy()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resource has changed`() {
        given(context.hasSpecificationChanged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).onResourceChanged()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when parallelism has changed`() {
        given(context.hasParallelismChanged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).hasParallelismChanged()
        inOrder.verify(context, times(1)).onResourceChanged()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job finished`() {
        given(context.isJobFinished()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).hasParallelismChanged()
        inOrder.verify(context, times(1)).isJobFinished()
        inOrder.verify(context, times(1)).onJobFinished()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job canceled`() {
        given(context.isJobCancelled()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).hasParallelismChanged()
        inOrder.verify(context, times(1)).isJobFinished()
        inOrder.verify(context, times(1)).isJobCancelled()
        inOrder.verify(context, times(1)).onJobCanceled()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job failed`() {
        given(context.isJobFailed()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).hasParallelismChanged()
        inOrder.verify(context, times(1)).isJobFinished()
        inOrder.verify(context, times(1)).isJobCancelled()
        inOrder.verify(context, times(1)).isJobFailed()
        inOrder.verify(context, times(1)).onJobFailed()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when manual action is present`() {
        given(context.isActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).hasParallelismChanged()
        inOrder.verify(context, times(1)).isJobFinished()
        inOrder.verify(context, times(1)).isJobCancelled()
        inOrder.verify(context, times(1)).isJobFailed()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).executeAction(setOf(
            Action.START, Action.STOP, Action.FORGET_SAVEPOINT, Action.TRIGGER_SAVEPOINT
        ))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when there is a savepoint in progress`() {
        given(context.updateSavepoint()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq("HEALTHY"))
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).hasParallelismChanged()
        inOrder.verify(context, times(1)).isJobFinished()
        inOrder.verify(context, times(1)).isJobCancelled()
        inOrder.verify(context, times(1)).isJobFailed()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).updateSavepoint()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(false))
        inOrder.verify(context, times(1)).updateJobStatus()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster has not started`() {
        given(context.isClusterStarted()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).isClusterStopping()
        inOrder.verify(context, times(1)).isClusterStopped()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth(eq(""))
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).hasParallelismChanged()
        inOrder.verify(context, times(1)).isJobFinished()
        inOrder.verify(context, times(1)).isJobCancelled()
        inOrder.verify(context, times(1)).isJobFailed()
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).updateSavepoint()
        inOrder.verify(context, times(1)).setResourceUpdated(eq(true))
        inOrder.verify(context, times(1)).updateJobStatus()
        verifyNoMoreInteractions(context)
    }


    @Test
    fun `should behave as expected when cluster is terminated`() {
        given(context.isClusterTerminated()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).onClusterStopping()
        verifyNoMoreInteractions(context)
    }
}