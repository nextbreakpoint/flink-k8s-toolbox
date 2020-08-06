package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkAddress
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import com.nextbreakpoint.flink.testing.TestFactory
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterGetJobStatusTest {
    private val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "123")
    private val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val kubeClient = mock(KubeClient::class.java)
    private val context = ClusterContext(cluster)
    private val command = ClusterGetJobStatus(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        val jobDetailsInfo1 = JobDetailsInfo()
        jobDetailsInfo1.state = JobDetailsInfo.StateEnum.RUNNING
        val jobDetailsInfo2 = JobDetailsInfo()
        jobDetailsInfo2.state = JobDetailsInfo.StateEnum.CANCELED
        given(flinkClient.getJobDetails(eq(flinkAddress), eq("123"))).thenReturn(jobDetailsInfo1).thenReturn(jobDetailsInfo2)
    }

    @Test
    fun `should return expected result when it can't fetch job status`() {
        given(flinkClient.getJobDetails(eq(flinkAddress), eq("123"))).thenThrow(RuntimeException())
        val result = command.execute(clusterSelector, "123")
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getJobDetails(eq(flinkAddress), eq("123"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when it can fetch job status`() {
        val result1 = command.execute(clusterSelector, "123")
        val result2 = command.execute(clusterSelector, "123")
        verify(kubeClient, times(2)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(2)).getJobDetails(eq(flinkAddress), eq("123"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result1).isNotNull()
        assertThat(result1.status).isEqualTo(ResultStatus.OK)
        assertThat(result1.output).isEqualTo("RUNNING")
        assertThat(result2).isNotNull()
        assertThat(result2.status).isEqualTo(ResultStatus.OK)
        assertThat(result2.output).isEqualTo("CANCELED")
    }
}