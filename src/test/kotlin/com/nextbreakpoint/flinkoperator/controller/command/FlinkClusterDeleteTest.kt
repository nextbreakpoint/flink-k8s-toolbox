package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import io.kubernetes.client.ApiResponse
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class FlinkClusterDeleteTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val command = FlinkClusterDelete(flinkOptions, flinkContext, kubernetesContext)

    @BeforeEach
    fun configure() {
    }

    @Test
    fun `should fail when kubernetesContext throws exception`() {
        given(kubernetesContext.deleteFlinkCluster(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).deleteFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when cluster resource can't be deleted`() {
        val response = mock(ApiResponse::class.java) as ApiResponse<Any>
        given(response.statusCode).thenReturn(500)
        given(kubernetesContext.deleteFlinkCluster(eq(clusterId))).thenReturn(response)
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).deleteFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should delete cluster resource`() {
        val response = mock(ApiResponse::class.java) as ApiResponse<Any>
        given(response.statusCode).thenReturn(200)
        given(kubernetesContext.deleteFlinkCluster(eq(clusterId))).thenReturn(response)
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).deleteFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
    }
}