package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class OnStartingTest {
    private val context = mock(TaskContext::class.java)
    private val task = OnStarting()
    private val actions = setOf(
        ManualAction.STOP,
        ManualAction.FORGET_SAVEPOINT
    )

    @BeforeEach
    fun configure() {
        given(context.isResourceDeleted()).thenReturn(false)
        given(context.hasTaskTimedOut()).thenReturn(false)
        given(context.isManualActionPresent()).thenReturn(false)
        given(context.hasResourceChanged()).thenReturn(false)
        given(context.hasScaleChanged()).thenReturn(false)
        given(context.startCluster()).thenReturn(false)
        given(context.ensurePodsExists()).thenReturn(true)
        given(context.ensureServiceExist()).thenReturn(true)
    }

    @Test
    fun `should behave as expected when nothing happens`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).hasScaleChanged()
        inOrder.verify(context, times(1)).ensurePodsExists()
        inOrder.verify(context, times(1)).ensureServiceExist()
        inOrder.verify(context, times(1)).startCluster()
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
    fun `should behave as expected when manual action is present`() {
        given(context.isManualActionPresent()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).executeManualAction(KotlinMockito.eq(actions))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when resource has changed`() {
        given(context.hasResourceChanged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).onResourceChanged()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when scale has changed`() {
        given(context.hasScaleChanged()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).hasScaleChanged()
        inOrder.verify(context, times(1)).onResourceScaled()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when pods are not ready`() {
        given(context.ensurePodsExists()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).hasScaleChanged()
        inOrder.verify(context, times(1)).ensurePodsExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when service is not ready`() {
        given(context.ensureServiceExist()).thenReturn(false)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).hasScaleChanged()
        inOrder.verify(context, times(1)).ensurePodsExists()
        inOrder.verify(context, times(1)).ensureServiceExist()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should behave as expected when cluster has been started`() {
        given(context.startCluster()).thenReturn(true)
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).isResourceDeleted()
        inOrder.verify(context, times(1)).hasTaskTimedOut()
        inOrder.verify(context, times(1)).isManualActionPresent()
        inOrder.verify(context, times(1)).hasResourceChanged()
        inOrder.verify(context, times(1)).hasScaleChanged()
        inOrder.verify(context, times(1)).ensurePodsExists()
        inOrder.verify(context, times(1)).ensureServiceExist()
        inOrder.verify(context, times(1)).startCluster()
        inOrder.verify(context, times(1)).onClusterStarted()
        verifyNoMoreInteractions(context)
    }
}