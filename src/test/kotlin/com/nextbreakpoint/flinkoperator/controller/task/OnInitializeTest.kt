package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.apache.log4j.Logger
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*

class OnInitializeTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val logger = mock(Logger::class.java)
    private val context = mock(TaskContext::class.java)
    private val task = OnInitialize(logger)

    @BeforeEach
    fun configure() {
        given(context.clusterId).thenReturn(clusterId)
    }

    @Test
    fun `should initialize cluster and change status to starting`() {
        task.execute(context)
        verify(logger, times(0)).info(any())
        verify(logger, times(0)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).initializeAnnotations()
        verify(context, times(1)).initializeStatus()
        verify(context, times(1)).updateDigests()
        verify(context, times(1)).addFinalizer()
        verify(context, times(1)).setClusterStatus(ClusterStatus.Starting)
        verifyNoMoreInteractions(context)
    }
}