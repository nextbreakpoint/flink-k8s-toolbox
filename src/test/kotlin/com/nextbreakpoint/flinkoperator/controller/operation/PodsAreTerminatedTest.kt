package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
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
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = PodsAreTerminated(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.listJobManagerPods(eq(clusterId))).thenReturn(V1PodList())
        given(kubeClient.listTaskManagerPods(eq(clusterId))).thenReturn(V1PodList())
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.listJobManagerPods(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).listJobManagerPods(eq(clusterId))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there aren't pods running`() {
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).listJobManagerPods(eq(clusterId))
        verify(kubeClient, times(1)).listTaskManagerPods(eq(clusterId))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
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
        given(kubeClient.listJobManagerPods(eq(clusterId))).thenReturn(podList)
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).listJobManagerPods(eq(clusterId))
        verify(kubeClient, times(1)).listTaskManagerPods(eq(clusterId))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.RETRY)
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
        given(kubeClient.listTaskManagerPods(eq(clusterId))).thenReturn(podList)
        val result = command.execute(clusterId, null)
        verify(kubeClient, times(1)).listJobManagerPods(eq(clusterId))
        verify(kubeClient, times(1)).listTaskManagerPods(eq(clusterId))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.RETRY)
        assertThat(result.output).isNull()
    }
}