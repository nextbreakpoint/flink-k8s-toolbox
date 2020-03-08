package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1ServiceBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterCreateServiceTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val service = mock(V1Service::class.java)
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = ClusterCreateService(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        val service = V1ServiceBuilder().withNewMetadata().withName("xxx").endMetadata().build()
        given(kubeClient.createService(eq(clusterId), any())).thenReturn(service)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.createService(eq(clusterId), any())).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, service)
        verify(kubeClient, times(1)).createService(eq(clusterId), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result`() {
        val result = command.execute(clusterId, service)
        verify(kubeClient, times(1)).createService(eq(clusterId), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isEqualTo("xxx")
    }
}