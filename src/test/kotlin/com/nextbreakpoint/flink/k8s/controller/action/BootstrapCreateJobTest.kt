package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito.any
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1JobBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class BootstrapCreateJobTest {
    private val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "123")
    private val bootstrapJob = mock(V1Job::class.java)
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = BootstrapCreateJob(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        val job = V1JobBuilder().withNewMetadata().withName("xxx").endMetadata().build()
        given(kubeClient.createBootstrapJob(eq(clusterSelector), any())).thenReturn(job)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.createBootstrapJob(eq(clusterSelector), any())).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, bootstrapJob)
        verify(kubeClient, times(1)).createBootstrapJob(eq(clusterSelector), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should create bootstrap job`() {
        val result = command.execute(clusterSelector, bootstrapJob)
        verify(kubeClient, times(1)).createBootstrapJob(eq(clusterSelector), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isEqualTo("xxx")
    }
}