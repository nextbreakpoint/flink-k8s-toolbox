package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
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

class ArePodsRunningTest {
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = ArePodsRunning(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.listJobManagerPods(eq(clusterSelector))).thenReturn(V1PodList())
        given(kubeClient.listTaskManagerPods(eq(clusterSelector))).thenReturn(V1PodList())
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.listJobManagerPods(eq(clusterSelector))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, null)
        verify(kubeClient, times(1)).listJobManagerPods(eq(clusterSelector))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should return expected result when there aren't pods running`() {
        val result = command.execute(clusterSelector, null)
        verify(kubeClient, times(1)).listJobManagerPods(eq(clusterSelector))
        verify(kubeClient, times(1)).listTaskManagerPods(eq(clusterSelector))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should return expected result when only job manager is running`() {
        val containerStatus = V1ContainerStatusBuilder()
            .withNewState()
            .withNewRunning()
            .endRunning()
            .endState()
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
        given(kubeClient.listJobManagerPods(eq(clusterSelector))).thenReturn(podList)
        val result = command.execute(clusterSelector, null)
        verify(kubeClient, times(1)).listJobManagerPods(eq(clusterSelector))
        verify(kubeClient, times(1)).listTaskManagerPods(eq(clusterSelector))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should return expected result when only task manager is running`() {
        val containerStatus = V1ContainerStatusBuilder()
            .withNewState()
            .withNewRunning()
            .endRunning()
            .endState()
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
        given(kubeClient.listTaskManagerPods(eq(clusterSelector))).thenReturn(podList)
        val result = command.execute(clusterSelector, null)
        verify(kubeClient, times(1)).listJobManagerPods(eq(clusterSelector))
        verify(kubeClient, times(1)).listTaskManagerPods(eq(clusterSelector))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isFalse()
    }

    @Test
    fun `should return expected result when job manger and task manager are running`() {
        val containerStatus = V1ContainerStatusBuilder()
            .withNewState()
            .withNewRunning()
            .endRunning()
            .endState()
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
        given(kubeClient.listJobManagerPods(eq(clusterSelector))).thenReturn(podList)
        given(kubeClient.listTaskManagerPods(eq(clusterSelector))).thenReturn(podList)
        val result = command.execute(clusterSelector, null)
        verify(kubeClient, times(1)).listJobManagerPods(eq(clusterSelector))
        verify(kubeClient, times(1)).listTaskManagerPods(eq(clusterSelector))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isTrue()
    }
}