package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verifyNoMoreInteractions

class JobOnTerminatedTest {
    private val context = mock(JobManager::class.java)
    private val task = JobOnTerminated()

    @Test
    fun `should behave as expected`() {
        task.execute(context)
        verifyNoMoreInteractions(context)
    }
}