package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class SavepointGetStatusTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val command = SavepointGetStatus(flinkOptions, flinkContext, kubernetesContext)
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")

    @BeforeEach
    fun configure() {
        given(kubernetesContext.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        given(flinkContext.getPendingSavepointRequests(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))).thenReturn(listOf())
        given(flinkContext.getLatestSavepointPaths(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))).thenReturn(mapOf("100" to "file://tmp/000"))
    }

    @Test
    fun `should fail when kubernetesContext throws exception`() {
        given(kubernetesContext.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, savepointRequest)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isEqualTo("")
    }

    @Test
    fun `should return expected result when there are pending requests`() {
        given(flinkContext.getPendingSavepointRequests(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))).thenReturn(listOf("1000"))
        val result = command.execute(clusterId, savepointRequest)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).getPendingSavepointRequests(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isEqualTo("")
    }

    @Test
    fun `should return expected result when there aren't savepoints`() {
        given(flinkContext.getLatestSavepointPaths(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))).thenReturn(mapOf())
        val result = command.execute(clusterId, savepointRequest)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).getPendingSavepointRequests(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))
        verify(flinkContext, times(1)).getLatestSavepointPaths(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isEqualTo("")
    }

    @Test
    fun `should return expected result when there are savepoints`() {
        val result = command.execute(clusterId, savepointRequest)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).getPendingSavepointRequests(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))
        verify(flinkContext, times(1)).getLatestSavepointPaths(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isEqualTo("file://tmp/000")
    }
}