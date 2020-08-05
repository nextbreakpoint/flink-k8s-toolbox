package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkclient.model.ClusterOverviewWithVersion
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.ClusterScale
import com.nextbreakpoint.flinkoperator.common.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterIsReadyTest {
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val kubeClient = mock(KubeClient::class.java)
    private val clusterScale = ClusterScale(taskManagers = 2, taskSlots = 1)
    private val command = ClusterIsReady(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        val overview = ClusterOverviewWithVersion()
        overview.slotsAvailable = 0
        overview.taskmanagers = 0
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, clusterScale)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should fail when flinkClient throws exception`() {
        given(flinkClient.getOverview(eq(flinkAddress))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, clusterScale)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should return expected result when there aren't task managers`() {
        val overview = ClusterOverviewWithVersion()
        overview.slotsAvailable = 2
        overview.taskmanagers = 0
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
        val result = command.execute(clusterSelector, clusterScale)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should return expected result when there aren't available slots`() {
        val overview = ClusterOverviewWithVersion()
        overview.slotsAvailable = 0
        overview.taskmanagers = 2
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
        val result = command.execute(clusterSelector, clusterScale)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should return expected result when there are task managers and available slots`() {
        val overview = ClusterOverviewWithVersion()
        overview.slotsAvailable = 2
        overview.taskmanagers = 2
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
        val result = command.execute(clusterSelector, clusterScale)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isTrue()
    }
}