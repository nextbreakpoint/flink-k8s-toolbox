package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1JobBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class BootstrapCreateJobTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val bootstrapJob = mock(V1Job::class.java)
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = BootstrapCreateJob(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        val job = V1JobBuilder().withNewMetadata().withName("xxx").endMetadata().build()
        given(kubeClient.createBootstrapJob(eq(clusterId), any())).thenReturn(job)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.createBootstrapJob(eq(clusterId), any())).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, bootstrapJob)
        verify(kubeClient, times(1)).createBootstrapJob(eq(clusterId), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result`() {
        val result = command.execute(clusterId, bootstrapJob)
        verify(kubeClient, times(1)).createBootstrapJob(eq(clusterId), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isNull()
    }
}