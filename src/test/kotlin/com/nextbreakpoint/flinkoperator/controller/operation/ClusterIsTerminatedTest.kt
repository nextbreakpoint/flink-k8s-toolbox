package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkclient.model.ClusterOverviewWithVersion
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
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

class ClusterIsTerminatedTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = ClusterIsTerminated(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        val overview = ClusterOverviewWithVersion()
        overview.slotsTotal = 1
        overview.taskmanagers = 1
        overview.jobsRunning = 1
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when flinkClient throws exception`() {
        given(flinkClient.getOverview(eq(flinkAddress))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are task managers`() {
        val overview = ClusterOverviewWithVersion()
        overview.slotsTotal = 0
        overview.taskmanagers = 1
        overview.jobsRunning = 0
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.RETRY)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are task slots`() {
        val overview = ClusterOverviewWithVersion()
        overview.slotsTotal = 1
        overview.taskmanagers = 0
        overview.jobsRunning = 0
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.RETRY)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are running jobs`() {
        val overview = ClusterOverviewWithVersion()
        overview.slotsTotal = 0
        overview.taskmanagers = 0
        overview.jobsRunning = 1
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.RETRY)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there aren't task managers, task slots or running jobs`() {
        val overview = ClusterOverviewWithVersion()
        overview.slotsTotal = 0
        overview.taskmanagers = 0
        overview.jobsRunning = 0
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isNull()
    }
}