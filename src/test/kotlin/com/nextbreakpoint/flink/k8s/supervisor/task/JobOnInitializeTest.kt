package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions

class JobOnInitializeTest {
    private val context = mock(JobManager::class.java)
    private val task = JobOnInitialise()

    @Test
    fun `should behave as expected when initializing job`() {
        task.execute(context)
        val inOrder = inOrder(context)
        inOrder.verify(context, times(1)).onResourceInitialise()
        verifyNoMoreInteractions(context)
    }
}