package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.FlinkJobAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class RequestJobStopTest {
    private val job = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val context = JobContext(job)
    private val command = RequestJobStop(flinkOptions, flinkClient, kubeClient, context)

    @BeforeEach
    fun configure() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Terminated)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        KotlinMockito.given(kubeClient.updateJobAnnotations(eq("flink"), eq("test-test"), KotlinMockito.any())).thenThrow(RuntimeException::class.java)
        val result = command.execute("flink", "test", "test", StopOptions(withoutSavepoint = true))
        verify(kubeClient, times(1)).updateJobAnnotations(eq("flink"), eq("test-test"), KotlinMockito.any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when stopping without savepoint`() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Stopping)
        val actionTimestamp = FlinkJobAnnotations.getActionTimestamp(job)
        val result = command.execute("flink", "test", "test", StopOptions(withoutSavepoint = true))
        verify(kubeClient, times(1)).updateJobAnnotations(eq("flink"), eq("test-test"), KotlinMockito.any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(FlinkJobAnnotations.getRequestedAction(job)).isEqualTo(Action.STOP)
        assertThat(FlinkJobAnnotations.isWithoutSavepoint(job)).isEqualTo(true)
        assertThat(FlinkJobAnnotations.isDeleteResources(job)).isEqualTo(false)
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isNotEqualTo(actionTimestamp)
    }

    @Test
    fun `should return expected result when stopping with savepoint`() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Stopping)
        val actionTimestamp = FlinkJobAnnotations.getActionTimestamp(job)
        val result = command.execute("flink", "test", "test", StopOptions(withoutSavepoint = false))
        verify(kubeClient, times(1)).updateJobAnnotations(eq("flink"), eq("test-test"), KotlinMockito.any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(FlinkJobAnnotations.getRequestedAction(job)).isEqualTo(Action.STOP)
        assertThat(FlinkJobAnnotations.isWithoutSavepoint(job)).isEqualTo(false)
        assertThat(FlinkJobAnnotations.isDeleteResources(job)).isEqualTo(false)
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isNotEqualTo(actionTimestamp)
    }
}