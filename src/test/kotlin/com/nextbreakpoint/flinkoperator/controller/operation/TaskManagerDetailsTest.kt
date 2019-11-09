package com.nextbreakpoint.flinkoperator.controller.operation

import com.google.gson.Gson
import com.nextbreakpoint.flinkclient.model.TaskManagerDetailsInfo
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class TaskManagerDetailsTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val taskManagerId = TaskManagerId("1")
    private val command = TaskManagerDetails(flinkOptions, flinkContext, kubernetesContext)

    @BeforeEach
    fun configure() {
        given(kubernetesContext.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        val taskManagerDetailsInfo = TaskManagerDetailsInfo()
        taskManagerDetailsInfo.id = "1"
        taskManagerDetailsInfo.slotsNumber = 4
        given(flinkContext.getTaskManagerDetails(eq(flinkAddress), eq(taskManagerId))).thenReturn(taskManagerDetailsInfo)
    }

    @Test
    fun `should fail when kubernetesContext throws exception`() {
        given(kubernetesContext.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, taskManagerId)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isEqualTo("{}")
    }

    @Test
    fun `should return expected result`() {
        val result = command.execute(clusterId, taskManagerId)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).getTaskManagerDetails(eq(flinkAddress), eq(taskManagerId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        val details = Gson().fromJson(result.output, TaskManagerDetailsInfo::class.java)
        assertThat(details.id).isEqualTo("1")
        assertThat(details.slotsNumber).isEqualTo(4)
    }
}