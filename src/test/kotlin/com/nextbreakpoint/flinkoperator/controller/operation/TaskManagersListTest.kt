package com.nextbreakpoint.flinkoperator.controller.operation

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import com.nextbreakpoint.flinkclient.model.TaskManagersInfo
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import io.kubernetes.client.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class TaskManagersListTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val command = TaskManagersList(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        val taskManagerInfo = TaskManagerInfo()
        taskManagerInfo.id = "1"
        taskManagerInfo.slotsNumber = 4
        val taskManagersInfo = TaskManagersInfo()
        taskManagersInfo.taskmanagers = listOf(taskManagerInfo)
        given(flinkClient.getTaskManagersOverview(eq(flinkAddress))).thenReturn(taskManagersInfo)
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
        assertThat(result.output).isEqualTo("{}")
    }

    @Test
    fun `should return expected result`() {
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getTaskManagersOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isNotNull()
        val overview: List<TaskManagerInfo> = JSON().deserialize(result.output, object : TypeToken<List<TaskManagerInfo>>() {}.type)
        assertThat(overview).hasSize(1)
        assertThat(overview[0].id).isEqualTo("1")
        assertThat(overview[0].slotsNumber).isEqualTo(4)
    }
}