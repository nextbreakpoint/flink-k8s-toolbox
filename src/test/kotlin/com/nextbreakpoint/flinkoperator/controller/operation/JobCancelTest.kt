package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkclient.model.CheckpointingStatistics
import com.nextbreakpoint.flinkclient.model.CheckpointingStatisticsCounts
import com.nextbreakpoint.flinkclient.model.TriggerResponse
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
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
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val options = SavepointOptions(targetPath = "/tmp")
    private val triggerResponse = TriggerResponse().requestId("100")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val command = JobCancel(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        given(flinkClient.listRunningJobs(eq(flinkAddress))).thenReturn(listOf("1"))
        val checkpointingStatistics = CheckpointingStatistics()
        checkpointingStatistics.counts = CheckpointingStatisticsCounts()
        checkpointingStatistics.counts.inProgress = 0
        given(flinkClient.getCheckpointingStatistics(eq(flinkAddress), eq(listOf("1")))).thenReturn(mapOf("1" to checkpointingStatistics))
        given(flinkClient.createSavepoint(eq(flinkAddress), eq("1"), eq("/tmp"))).thenReturn(triggerResponse)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isEqualTo(SavepointRequest("", ""))
    }

    @Test
    fun `should return expected result when there aren't running jobs`() {
        given(flinkClient.listRunningJobs(eq(flinkAddress))).thenReturn(listOf())
        val result = command.execute(clusterId, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listRunningJobs(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isEqualTo(SavepointRequest("", ""))
    }

    @Test
    fun `should return expected result when there is only one job running and there are checkpoints in progress`() {
        val checkpointingStatistics = CheckpointingStatistics()
        checkpointingStatistics.counts = CheckpointingStatisticsCounts()
        checkpointingStatistics.counts.inProgress = 1
        given(flinkClient.getCheckpointingStatistics(eq(flinkAddress), eq(listOf("1")))).thenReturn(mapOf("1" to checkpointingStatistics))
        val result = command.execute(clusterId, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listRunningJobs(eq(flinkAddress))
        verify(flinkClient, times(1)).getCheckpointingStatistics(eq(flinkAddress), eq(listOf("1")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isEqualTo(SavepointRequest("", ""))
    }

    @Test
    fun `should return expected result when there is only one job running and there aren't checkpoints in progress`() {
        val result = command.execute(clusterId, options)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listRunningJobs(eq(flinkAddress))
        verify(flinkClient, times(1)).getCheckpointingStatistics(eq(flinkAddress), eq(listOf("1")))
        verify(flinkClient, times(1)).createSavepoint(eq(flinkAddress), eq("1"), eq("/tmp"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isEqualTo(savepointRequest)
    }
}