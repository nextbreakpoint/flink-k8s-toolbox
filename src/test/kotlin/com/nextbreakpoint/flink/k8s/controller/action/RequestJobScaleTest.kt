package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ScaleJobOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.FlinkJobAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class RequestJobScaleTest {
    private val job = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val supervisorCache = mock(Cache::class.java)
    private val command = RequestJobScale(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.rescaleJob(eq("flink"), eq("test-test"), eq(4))).thenThrow(RuntimeException::class.java)
        val result = command.execute("flink", "test", "test", ScaleJobOptions(parallelism = 4))
        verify(kubeClient, times(1)).rescaleJob(eq("flink"), eq("test-test"), eq(4))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(supervisorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when scaling`() {
        val actionTimestamp = FlinkJobAnnotations.getActionTimestamp(job)
        val result = command.execute("flink", "test", "test", ScaleJobOptions(parallelism = 4))
        verify(kubeClient, times(1)).rescaleJob(eq("flink"), eq("test-test"), eq(4))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(supervisorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isEqualTo(actionTimestamp)
    }

    @Test
    fun `should return expected result when scaling down to zero`() {
        val actionTimestamp = FlinkJobAnnotations.getActionTimestamp(job)
        val result = command.execute("flink", "test", "test", ScaleJobOptions(parallelism = 0))
        verify(kubeClient, times(1)).rescaleJob(eq("flink"), eq("test-test"), eq(0))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(supervisorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isEqualTo(actionTimestamp)
    }
}