package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
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

class SavepointTriggerTest {
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val savepointOptions = SavepointOptions(targetPath = "file://tmp")
    private val command = SavepointTrigger(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        given(flinkClient.listRunningJobs(eq(flinkAddress))).thenReturn(listOf("1"))
        given(flinkClient.isCheckpointInProgress(eq(flinkAddress), eq(listOf("1")))).thenReturn(listOf())
        given(flinkClient.triggerSavepoints(eq(flinkAddress), eq(listOf("1")), eq("file://tmp"))).thenReturn(mapOf("1" to "100"))
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, savepointOptions)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.ERROR)
        assertThat(result.output).isEqualTo(SavepointRequest("", ""))
    }

    @Test
    fun `should return expected result when there aren't running jobs`() {
        given(flinkClient.listRunningJobs(eq(flinkAddress))).thenReturn(listOf())
        val result = command.execute(clusterSelector, savepointOptions)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listRunningJobs(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.ERROR)
        assertThat(result.output).isEqualTo(SavepointRequest("", ""))
    }

    @Test
    fun `should return expected result when there are savepoints in progress`() {
        given(flinkClient.isCheckpointInProgress(eq(flinkAddress), eq(listOf("1")))).thenReturn(listOf("1"))
        val result = command.execute(clusterSelector, savepointOptions)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listRunningJobs(eq(flinkAddress))
        verify(flinkClient, times(1)).isCheckpointInProgress(eq(flinkAddress), eq(listOf("1")))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.ERROR)
        assertThat(result.output).isEqualTo(SavepointRequest("", ""))
    }

    @Test
    fun `should return expected result when there aren't savepoints in progress`() {
        val result = command.execute(clusterSelector, savepointOptions)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listRunningJobs(eq(flinkAddress))
        verify(flinkClient, times(1)).isCheckpointInProgress(eq(flinkAddress), eq(listOf("1")))
        verify(flinkClient, times(1)).triggerSavepoints(eq(flinkAddress), eq(listOf("1")), eq("file://tmp"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.OK)
        assertThat(result.output).isEqualTo(SavepointRequest(jobId = "1", triggerId = "100"))
    }

    @Test
    fun `should return expected result when there are multiple jobs running`() {
        given(flinkClient.listRunningJobs(eq(flinkAddress))).thenReturn(listOf("1", "2"))
        val result = command.execute(clusterSelector, savepointOptions)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).listRunningJobs(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.ERROR)
        assertThat(result.output).isEqualTo(SavepointRequest("", ""))
    }
}