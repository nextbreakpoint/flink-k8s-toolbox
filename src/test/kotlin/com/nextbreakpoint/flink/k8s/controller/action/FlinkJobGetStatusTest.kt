package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verifyNoMoreInteractions

class FlinkJobGetStatusTest {
    private val job = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val context = JobContext(job)
    private val command = FlinkJobGetStatus(flinkOptions, flinkClient, kubeClient, context)

    @BeforeEach
    fun configure() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
    }

    @Test
    fun `should return job status`() {
        val result = command.execute("flink", "test", "test", null)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNotNull()
        assertThat(result.output?.supervisorStatus).isEqualTo("Started")
    }
}