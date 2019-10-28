package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import io.kubernetes.client.models.V1ContainerStatusBuilder
import io.kubernetes.client.models.V1PodList
import io.kubernetes.client.models.V1PodListBuilder
import io.kubernetes.client.models.V1PodStatusBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class PodsAreTerminatedTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val command = PodsAreTerminated(flinkOptions, flinkContext, kubernetesContext)

    @BeforeEach
    fun configure() {
        given(kubernetesContext.listJobManagerPods(eq(clusterId))).thenReturn(V1PodList())
        given(kubernetesContext.listTaskManagerPods(eq(clusterId))).thenReturn(V1PodList())
    }

    @Test
    fun `should fail when kubernetesContext throws exception`() {
        given(kubernetesContext.listJobManagerPods(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).listJobManagerPods(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there aren't pods running`() {
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).listJobManagerPods(eq(clusterId))
        verify(kubernetesContext, times(1)).listTaskManagerPods(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are job manager still running`() {
        val containerStatus = V1ContainerStatusBuilder()
            .withNewLastState()
            .withNewRunning()
            .endRunning()
            .endLastState()
            .build()
        val podStatus = V1PodStatusBuilder()
            .addToContainerStatuses(containerStatus)
            .build()
        val podList = V1PodListBuilder()
            .addNewItem()
            .withNewSpec()
            .endSpec()
            .withStatus(podStatus)
            .endItem()
            .build()
        given(kubernetesContext.listJobManagerPods(eq(clusterId))).thenReturn(podList)
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).listJobManagerPods(eq(clusterId))
        verify(kubernetesContext, times(1)).listTaskManagerPods(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are task manager still running`() {
        val containerStatus = V1ContainerStatusBuilder()
            .withNewLastState()
            .withNewRunning()
            .endRunning()
            .endLastState()
            .build()
        val podStatus = V1PodStatusBuilder()
            .addToContainerStatuses(containerStatus)
            .build()
        val podList = V1PodListBuilder()
            .addNewItem()
            .withNewSpec()
            .endSpec()
            .withStatus(podStatus)
            .endItem()
            .build()
        given(kubernetesContext.listTaskManagerPods(eq(clusterId))).thenReturn(podList)
        val result = command.execute(clusterId, null)
        verify(kubernetesContext, times(1)).listJobManagerPods(eq(clusterId))
        verify(kubernetesContext, times(1)).listTaskManagerPods(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
    }
}