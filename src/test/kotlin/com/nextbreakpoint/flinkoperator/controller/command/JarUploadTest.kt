package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1JobBuilder
import io.kubernetes.client.models.V1JobListBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class JarUploadTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val resources = mock(ClusterResources::class.java)
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val command = JarUpload(flinkOptions, flinkContext, kubernetesContext)

    @BeforeEach
    fun configure() {
        val job = V1JobBuilder().withNewMetadata().withName("xxx").endMetadata().build()
        given(kubernetesContext.createUploadJob(eq(clusterId), any())).thenReturn(job)
    }

    @Test
    fun `should fail when kubernetesContext throws exception`() {
        given(kubernetesContext.listUploadJobs(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, resources)
        verify(kubernetesContext, times(1)).listUploadJobs(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are jobs`() {
        val jobs = V1JobListBuilder().addNewItem().endItem().build()
        given(kubernetesContext.listUploadJobs(eq(clusterId))).thenReturn(jobs)
        val result = command.execute(clusterId, resources)
        verify(kubernetesContext, times(1)).listUploadJobs(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there aren't jobs`() {
        val jobs = V1JobListBuilder().build()
        given(kubernetesContext.listUploadJobs(eq(clusterId))).thenReturn(jobs)
        val result = command.execute(clusterId, resources)
        verify(kubernetesContext, times(1)).listUploadJobs(eq(clusterId))
        verify(kubernetesContext, times(1)).createUploadJob(eq(clusterId), any())
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
    }
}