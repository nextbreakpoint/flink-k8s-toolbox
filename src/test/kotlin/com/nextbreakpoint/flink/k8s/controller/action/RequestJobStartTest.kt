package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.FlinkJobAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito
import com.nextbreakpoint.flink.testing.KotlinMockito.any
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class RequestJobStartTest {
    private val jobSelector = ResourceSelector(namespace = "flink", name = "test-test", uid = "123")
    private val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
    private val job = TestFactory.aFlinkJob(cluster)
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val context = JobContext(job)
    private val command = RequestJobStart(flinkOptions, flinkClient, kubeClient, context)

    @BeforeEach
    fun configure() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        KotlinMockito.given(kubeClient.updateJobAnnotations(eq(jobSelector), any())).thenThrow(RuntimeException::class.java)
        val result = command.execute(jobSelector, StartOptions(withoutSavepoint = true))
        verify(kubeClient, times(1)).updateJobAnnotations(eq(jobSelector), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when starting without savepoint`() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Terminated)
        val actionTimestamp = FlinkJobAnnotations.getActionTimestamp(job)
        val result = command.execute(jobSelector, StartOptions(withoutSavepoint = true))
        verify(kubeClient, times(1)).updateJobAnnotations(eq(jobSelector), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(FlinkJobAnnotations.getManualAction(job)).isEqualTo(ManualAction.START)
        assertThat(FlinkJobAnnotations.isWithoutSavepoint(job)).isEqualTo(true)
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isNotEqualTo(actionTimestamp)
    }

    @Test
    fun `should return expected result when starting with savepoint`() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Terminated)
        val actionTimestamp = FlinkJobAnnotations.getActionTimestamp(job)
        val result = command.execute(jobSelector, StartOptions(withoutSavepoint = false))
        verify(kubeClient, times(1)).updateJobAnnotations(eq(jobSelector), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(FlinkJobAnnotations.getManualAction(job)).isEqualTo(ManualAction.START)
        assertThat(FlinkJobAnnotations.isWithoutSavepoint(job)).isEqualTo(false)
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isNotEqualTo(actionTimestamp)
    }
}