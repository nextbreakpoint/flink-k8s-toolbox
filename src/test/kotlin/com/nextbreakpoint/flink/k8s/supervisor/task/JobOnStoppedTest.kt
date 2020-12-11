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

class JobOnStoppedTest {
    private val context = mock(JobManager::class.java)
    private val task = JobOnStopped()

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.terminateBootstrapJob()).thenReturn(true)
        given(context.isClusterStarted()).thenReturn(true)
        given(context.isClusterStarting()).thenReturn(false)
        given(context.isClusterTerminated()).thenReturn(false)
        given(context.isClusterUnhealthy()).thenReturn(false)
        given(context.shouldRestartJob()).thenReturn(true)
        given(context.hasSpecificationChanged()).thenReturn(false)
        given(context.isActionPresent()).thenReturn(false)
        given(context.isReadyToRestart()).thenReturn(true)
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
    fun `should behave as expected when bootstrap job hasn't been deleted yet`() {
        given(context.terminateBootstrapJob()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster hasn't started`() {
        given(context.isClusterStarted()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).setClusterHealth("")
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster is unhealthy`() {
        given(context.isClusterUnhealthy()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth(eq("UNHEALTHY"))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job shouldn't restart`() {
        given(context.shouldRestartJob()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).shouldRestartJob()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resource has changed and job has finalizer`() {
        given(context.hasSpecificationChanged()).thenReturn(true)
        given(context.hasFinalizer()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).shouldRestartJob()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).hasFinalizer()
        inOrder.verify(context, times(1)).onResourceChanged()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resource has changed and job doesn't have finalizer`() {
        given(context.hasSpecificationChanged()).thenReturn(true)
        given(context.hasFinalizer()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).shouldRestartJob()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).hasFinalizer()
        inOrder.verify(context, times(1)).addFinalizer()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when manual action is present`() {
        given(context.isActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).executeAction(setOf(Action.START))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job should restart but timeout didn't occur yet`() {
        given(context.isReadyToRestart()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).shouldRestartJob()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isReadyToRestart()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job should restart and timeout occurred and job has finalizer`() {
        given(context.isReadyToRestart()).thenReturn(true)
        given(context.hasFinalizer()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).shouldRestartJob()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isReadyToRestart()
        inOrder.verify(context, times(1)).hasFinalizer()
        inOrder.verify(context, times(1)).onJobReadyToRestart()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job should restart and timeout occurred and job hasn't finalizer`() {
        given(context.isReadyToRestart()).thenReturn(true)
        given(context.hasFinalizer()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).terminateBootstrapJob()
        inOrder.verify(context, times(1)).isClusterStarting()
        inOrder.verify(context, times(1)).isClusterStarted()
        inOrder.verify(context, times(1)).isClusterUnhealthy()
        inOrder.verify(context, times(1)).setClusterHealth("HEALTHY")
        inOrder.verify(context, times(1)).isActionPresent()
        inOrder.verify(context, times(1)).shouldRestartJob()
        inOrder.verify(context, times(1)).hasSpecificationChanged()
        inOrder.verify(context, times(1)).isReadyToRestart()
        inOrder.verify(context, times(1)).hasFinalizer()
        inOrder.verify(context, times(1)).addFinalizer()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job luster is terminate and job doesn't have finalizer`() {
        given(context.isClusterTerminated()).thenReturn(true)
        given(context.hasFinalizer()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).hasFinalizer()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when job luster is terminate and job has finalizer`() {
        given(context.isClusterTerminated()).thenReturn(true)
        given(context.hasFinalizer()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).isClusterTerminated()
        inOrder.verify(context, times(1)).hasFinalizer()
        inOrder.verify(context, times(1)).removeFinalizer()
        verifyNoMoreInteractions(context)
    }
}