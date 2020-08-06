package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterOnTerminatedTest {
    private val context = mock(ClusterManager::class.java)
    private val task = ClusterOnTerminated()

    @Test
    fun `should behave as expected`() {
        task.execute(context)
        verifyNoMoreInteractions(context)
    }
}