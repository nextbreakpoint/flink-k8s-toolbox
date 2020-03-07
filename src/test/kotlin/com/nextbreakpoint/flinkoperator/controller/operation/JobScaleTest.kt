package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkclient.model.TriggerResponse
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.*

class JobScaleTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = JobScale(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        given(flinkClient.listJars(eq(flinkAddress))).thenReturn(listOf())
        Status.setJobParallelism(cluster, 1)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, 2)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when flinkClient throws exception`() {
        given(flinkClient.triggerJobRescaling(eq(flinkAddress), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt())).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, 2)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).triggerJobRescaling(eq(flinkAddress), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result`() {
        given(flinkClient.triggerJobRescaling(eq(flinkAddress), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt())).thenReturn(TriggerResponse())
        val result = command.execute(clusterId, 3)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).triggerJobRescaling(eq(flinkAddress), ArgumentMatchers.anyString(), ArgumentMatchers.eq(3))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isNull()
    }
}