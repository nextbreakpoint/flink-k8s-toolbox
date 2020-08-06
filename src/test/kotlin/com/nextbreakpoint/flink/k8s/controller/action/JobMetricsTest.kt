package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkAddress
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.JobStats
import com.nextbreakpoint.flink.common.Metric
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito.any
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import io.kubernetes.client.openapi.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class JobMetricsTest {
    private val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val command = JobMetrics(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        given(flinkClient.getJobMetrics(eq(flinkAddress), eq("1"), any())).thenReturn(listOf(Metric("totalNumberOfCheckpoints", "10"), Metric("lastCheckpointSize", "100")))
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, "1")
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isEqualTo("{}")
    }

    @Test
    fun `should return expected result when it can't fetch job's metrics`() {
        given(flinkClient.getJobMetrics(eq(flinkAddress), eq("1"), any())).thenThrow(RuntimeException())
        val result = command.execute(clusterSelector, "1")
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getJobMetrics(eq(flinkAddress), eq("1"), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isEqualTo("{}")
    }

    @Test
    fun `should return expected result when it can fetch job's metrics`() {
        val result = command.execute(clusterSelector, "1")
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getJobMetrics(eq(flinkAddress), eq("1"), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        val metrics = JSON().deserialize<JobStats>(result.output, JobStats::class.java)
        assertThat(metrics.totalNumberOfCheckpoints).isEqualTo(10)
        assertThat(metrics.lastCheckpointSize).isEqualTo(100)
    }
}