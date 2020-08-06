package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterOnInitializeTest {
    private val context = mock(ClusterManager::class.java)
    private val task = ClusterOnInitialize()

    @Test
    fun `should behave as expected`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).onResourceInitialise()
        verifyNoMoreInteractions(context)
    }
}