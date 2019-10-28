package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkclient.model.CheckpointingStatistics
import com.nextbreakpoint.flinkclient.model.CheckpointingStatisticsCounts
import com.nextbreakpoint.flinkclient.model.TriggerResponse
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
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

class JobCancelTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val options = SavepointOptions(targetPath = "/tmp")
    private val triggerResponse = TriggerResponse().requestId("100")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val command = JobCancel(flinkOptions, flinkContext, kubernetesContext)

    @BeforeEach
    fun configure() {
        given(kubernetesContext.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        given(flinkContext.listRunningJobs(eq(flinkAddress))).thenReturn(listOf("1"))
        val checkpointingStatistics = CheckpointingStatistics()
        checkpointingStatistics.counts = CheckpointingStatisticsCounts()
        checkpointingStatistics.counts.inProgress = 0
        given(flinkContext.getCheckpointingStatistics(eq(flinkAddress), eq(listOf("1")))).thenReturn(mapOf("1" to checkpointingStatistics))
        given(flinkContext.createSavepoint(eq(flinkAddress), eq("1"), eq("/tmp"))).thenReturn(triggerResponse)
    }

    @Test
    fun `should fail when kubernetesContext throws exception`() {
        given(kubernetesContext.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, options)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there aren't running jobs`() {
        given(flinkContext.listRunningJobs(eq(flinkAddress))).thenReturn(listOf())
        val result = command.execute(clusterId, options)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).listRunningJobs(eq(flinkAddress))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there is only one job running and there are checkpoints in progress`() {
        val checkpointingStatistics = CheckpointingStatistics()
        checkpointingStatistics.counts = CheckpointingStatisticsCounts()
        checkpointingStatistics.counts.inProgress = 1
        given(flinkContext.getCheckpointingStatistics(eq(flinkAddress), eq(listOf("1")))).thenReturn(mapOf("1" to checkpointingStatistics))
        val result = command.execute(clusterId, options)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).listRunningJobs(eq(flinkAddress))
        verify(flinkContext, times(1)).getCheckpointingStatistics(eq(flinkAddress), eq(listOf("1")))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there is only one job running and there aren't checkpoints in progress`() {
        val result = command.execute(clusterId, options)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).listRunningJobs(eq(flinkAddress))
        verify(flinkContext, times(1)).getCheckpointingStatistics(eq(flinkAddress), eq(listOf("1")))
        verify(flinkContext, times(1)).createSavepoint(eq(flinkAddress), eq("1"), eq("/tmp"))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isEqualTo(savepointRequest)
    }
}