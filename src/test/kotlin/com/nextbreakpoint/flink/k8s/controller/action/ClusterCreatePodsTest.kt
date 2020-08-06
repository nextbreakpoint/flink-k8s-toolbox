package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.PodReplicas
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito.any
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import com.nextbreakpoint.flink.testing.TestFactory
import io.kubernetes.client.openapi.models.V1PodBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterCreatePodsTest {
    private val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
    private val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val podReplicas = PodReplicas(pod = TestFactory.aTaskManagerPod(cluster, "1"), replicas = 4)
    private val command = ClusterCreatePods(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        val pod1 = V1PodBuilder().withNewMetadata().withName("1").endMetadata().build()
        val pod2 = V1PodBuilder().withNewMetadata().withName("2").endMetadata().build()
        val pod3 = V1PodBuilder().withNewMetadata().withName("3").endMetadata().build()
        val pod4 = V1PodBuilder().withNewMetadata().withName("4").endMetadata().build()
        given(kubeClient.createPod(eq(clusterSelector), any()))
            .thenReturn(pod1).thenReturn(pod2).thenReturn(pod3).thenReturn(pod4)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.createPod(eq(clusterSelector), any())).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, podReplicas)
        verify(kubeClient, times(1)).createPod(eq(clusterSelector), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isEmpty()
    }

    @Test
    fun `should create pods`() {
        val result = command.execute(clusterSelector, podReplicas)
        verify(kubeClient, times(4)).createPod(eq(clusterSelector), eq(podReplicas.pod))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).containsAll(setOf("1", "2", "3", "4"))
    }
}