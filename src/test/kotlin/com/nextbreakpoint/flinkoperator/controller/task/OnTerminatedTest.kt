package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class OnTerminatedTest {
    private val context = mock(TaskContext::class.java)
    private val task = OnTerminated()

    @Test
    fun `should behave as expected when nothing happens`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).terminateCluster()
        verifyNoMoreInteractions(context)
    }
}