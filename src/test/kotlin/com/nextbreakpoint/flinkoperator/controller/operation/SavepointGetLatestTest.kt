package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class SavepointGetLatestTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val command = SavepointGetLatest(flinkOptions, flinkClient, kubeClient)
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        given(flinkClient.getPendingSavepointRequests(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))).thenReturn(listOf())
        given(flinkClient.getLatestSavepointPaths(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))).thenReturn(mapOf("100" to "file://tmp/000"))
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, savepointRequest)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isEqualTo("")
    }

    @Test
    fun `should return expected result when there are pending requests`() {
        given(flinkClient.getPendingSavepointRequests(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))).thenReturn(listOf("1000"))
        val result = command.execute(clusterId, savepointRequest)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getPendingSavepointRequests(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.RETRY)
        assertThat(result.output).isEqualTo("")
    }

    @Test
    fun `should return expected result when there aren't savepoints`() {
        given(flinkClient.getLatestSavepointPaths(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))).thenReturn(mapOf())
        val result = command.execute(clusterId, savepointRequest)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getPendingSavepointRequests(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))
        verify(flinkClient, times(1)).getLatestSavepointPaths(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isEqualTo("")
    }

    @Test
    fun `should return expected result when there are savepoints`() {
        val result = command.execute(clusterId, savepointRequest)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getPendingSavepointRequests(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))
        verify(flinkClient, times(1)).getLatestSavepointPaths(eq(flinkAddress), eq(mapOf(savepointRequest.jobId to savepointRequest.triggerId)))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isEqualTo("file://tmp/000")
    }
}