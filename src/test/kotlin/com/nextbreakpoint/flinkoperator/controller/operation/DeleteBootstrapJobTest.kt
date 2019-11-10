package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
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

class DeleteBootstrapJobTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val command = DeleteBootstrapJob(flinkOptions, flinkContext, kubernetesContext)

    @BeforeEach
    fun configure() {
    }

    @Test
    fun `should fail when kubernetesContext throws exception`() {
        given(kubernetesContext.deleteBootstrapJobs(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).deleteBootstrapJobs(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should delete bootstrap job and pods`() {
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).deleteBootstrapJobs(eq(clusterId))
        verify(kubernetesContext, times(1)).deleteBootstrapJobPods(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
    }
}