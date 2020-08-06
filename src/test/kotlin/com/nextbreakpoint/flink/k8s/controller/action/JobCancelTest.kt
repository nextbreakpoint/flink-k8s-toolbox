package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flinkclient.model.TriggerResponse
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkAddress
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.SavepointOptions
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class JobCancelTest {
    private val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val context = mock(JobContext::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val options = SavepointOptions(targetPath = "/tmp")
    private val triggerResponse = TriggerResponse().requestId("100")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val command = JobCancel(flinkOptions, flinkClient, kubeClient, context)

    @BeforeEach
    fun configure() {
        given(context.getJobId()).thenReturn("1")
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        given(flinkClient.cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))).thenReturn(mapOf("1" to triggerResponse.requestId))
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when job can be cancelled`() {
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isEqualTo(savepointRequest)
    }

    @Test
    fun `should return expected result when job can't create savepoint`() {
        given(flinkClient.cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))).thenReturn(mapOf())
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))
        verify(flinkClient, times(1)).terminateJobs(eq(flinkAddress), eq(listOf("1")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when job can't be cancelled`() {
        given(flinkClient.cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))).thenThrow(RuntimeException())
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when job can't be stopped`() {
        given(flinkClient.cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))).thenReturn(mapOf())
        given(flinkClient.terminateJobs(eq(flinkAddress), eq(listOf("1")))).thenThrow(RuntimeException())
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))
        verify(flinkClient, times(1)).terminateJobs(eq(flinkAddress), eq(listOf("1")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }
}