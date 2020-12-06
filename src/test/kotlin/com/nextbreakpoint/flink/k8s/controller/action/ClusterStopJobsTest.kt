package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkAddress
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito.any
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import com.nextbreakpoint.flinkclient.model.JobIdWithStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterStopJobsTest {
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val command = ClusterStopJobs(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute("flink", "test", setOf())
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should return expected result when there aren't jobs`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf())
        val result = command.execute("flink", "test", setOf())
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isTrue()
    }

    @Test
    fun `should return expected result when there is one job running`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.RUNNING))
        val result = command.execute("flink", "test", setOf())
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verify(flinkClient, times(1)).terminateJobs(eq(flinkAddress), eq(listOf("1")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should return expected result when there is one job cancelled`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.CANCELED))
        val result = command.execute("flink", "test", setOf())
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isTrue()
    }

    @Test
    fun `should return expected result when there is one job restarting`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.RESTARTING))
        val result = command.execute("flink", "test", setOf())
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verify(flinkClient, times(1)).terminateJobs(eq(flinkAddress), eq(listOf("1")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should return expected result when there are two jobs running`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.RUNNING, "2" to JobIdWithStatus.StatusEnum.RUNNING))
        val result = command.execute("flink", "test", setOf())
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verify(flinkClient, times(1)).terminateJobs(eq(flinkAddress), eq(listOf("1", "2")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isFalse()
    }
}