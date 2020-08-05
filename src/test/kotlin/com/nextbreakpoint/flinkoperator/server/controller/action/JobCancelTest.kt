package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkclient.model.CheckpointingStatistics
import com.nextbreakpoint.flinkclient.model.CheckpointingStatisticsCounts
import com.nextbreakpoint.flinkclient.model.JobIdWithStatus
import com.nextbreakpoint.flinkclient.model.TriggerResponse
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.SavepointRequest
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class JobCancelTest {
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val options = SavepointOptions(targetPath = "/tmp")
    private val triggerResponse = TriggerResponse().requestId("100")
    private val savepointRequest1 = SavepointRequest(jobId = "1", triggerId = "100")
    private val savepointRequest2 = SavepointRequest(jobId = "2", triggerId = "100")
    private val command = JobCancel(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        val checkpointingStatistics = CheckpointingStatistics()
        checkpointingStatistics.counts = CheckpointingStatisticsCounts()
        checkpointingStatistics.counts.inProgress = 0
        given(flinkClient.getCheckpointingStatistics(eq(flinkAddress), eq(listOf("1")))).thenReturn(mapOf("1" to checkpointingStatistics))
        given(flinkClient.getCheckpointingStatistics(eq(flinkAddress), eq(listOf("2")))).thenReturn(mapOf("2" to checkpointingStatistics))
        given(flinkClient.cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))).thenReturn(mapOf("1" to triggerResponse.requestId))
        given(flinkClient.cancelJobs(eq(flinkAddress), eq(listOf("2")), eq("/tmp"))).thenReturn(mapOf("2" to triggerResponse.requestId))
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
    fun `should return expected result when there aren't jobs`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf())
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isEqualTo(SavepointRequest("", ""))
    }

    @Test
    fun `should return expected result when there is one job cancelling`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.CANCELLING))
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isEqualTo(SavepointRequest("", ""))
    }

    @Test
    fun `should return expected result when there is one job running`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.RUNNING))
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verify(flinkClient, times(1)).cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isEqualTo(savepointRequest1)
    }

    @Test
    fun `should return expected result when there is one job restarting`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.RESTARTING))
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verify(flinkClient, times(1)).terminateJobs(eq(flinkAddress), eq(listOf("1")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there is one job restarting and one job running`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.RUNNING, "2" to JobIdWithStatus.StatusEnum.RESTARTING))
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verify(flinkClient, times(1)).terminateJobs(eq(flinkAddress), eq(listOf("2")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there is one job cancelled and one job running`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.CANCELED, "2" to JobIdWithStatus.StatusEnum.RUNNING))
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verify(flinkClient, times(1)).cancelJobs(eq(flinkAddress), eq(listOf("2")), eq("/tmp"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isEqualTo(savepointRequest2)
    }

    @Test
    fun `should return expected result when there are two jobs running`() {
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.RUNNING, "2" to JobIdWithStatus.StatusEnum.RUNNING))
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verify(flinkClient, times(1)).terminateJobs(eq(flinkAddress), eq(listOf("1", "2")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there is one job running but job can't be cancelled`() {
        given(flinkClient.cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))).thenReturn(mapOf())
        given(flinkClient.listJobs(eq(flinkAddress), any())).thenReturn(mapOf("1" to JobIdWithStatus.StatusEnum.RUNNING))
        val result = command.execute(clusterSelector, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listJobs(eq(flinkAddress), any())
        verify(flinkClient, times(1)).cancelJobs(eq(flinkAddress), eq(listOf("1")), eq("/tmp"))
        verify(flinkClient, times(1)).terminateJobs(eq(flinkAddress), eq(listOf("1")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
    }
}