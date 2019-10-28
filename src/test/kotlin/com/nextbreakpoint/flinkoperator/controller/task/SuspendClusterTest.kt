package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class SuspendClusterTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster("test", "flink")
    private val context = mock(OperatorContext::class.java)
    private val controller = mock(OperatorController::class.java)
    private val resources = mock(OperatorResources::class.java)
    private val time = System.currentTimeMillis()
    private val task = SuspendCluster()

    @BeforeEach
    fun configure() {
        given(context.lastUpdated).thenReturn(time)
        given(context.controller).thenReturn(controller)
        given(context.resources).thenReturn(resources)
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
    }

    @Test
    fun `onExecuting should return expected result`() {
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should update cluster status`() {
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.SUSPENDED)
        assertThat(OperatorAnnotations.getOperatorTaskAttempts(cluster)).isEqualTo(0)
    }

    @Test
    fun `onAwaiting should return expected result`() {
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onIdle should return expected result`() {
        val result = task.onIdle(context)
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onFailed should return expected result`() {
        val result = task.onFailed(context)
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }
}