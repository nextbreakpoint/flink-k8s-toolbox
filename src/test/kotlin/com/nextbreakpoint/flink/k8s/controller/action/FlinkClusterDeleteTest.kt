package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import io.kubernetes.client.openapi.ApiResponse
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class FlinkClusterDeleteTest {
    private val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = FlinkClusterDelete(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.deleteFlinkClusterV2(eq(clusterSelector))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, null)
        verify(kubeClient, times(1)).deleteFlinkClusterV2(eq(clusterSelector))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should delete flink cluster`() {
        val result = command.execute(clusterSelector, null)
        verify(kubeClient, times(1)).deleteFlinkClusterV2(eq(clusterSelector))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
    }
}