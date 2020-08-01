package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.PodReplicas
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import io.kubernetes.client.models.V1PodBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterCreatePodsTest {
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val podReplicas = PodReplicas(pod = TestFactory.aTaskManagerPod(cluster,"1"), replicas = 4)
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
        assertThat(result.status).isEqualTo(OperationStatus.ERROR)
        assertThat(result.output).isEmpty()
    }

    @Test
    fun `should return expected result`() {
        val result = command.execute(clusterSelector, podReplicas)
        verify(kubeClient, times(4)).createPod(eq(clusterSelector), eq(podReplicas.pod))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.OK)
        assertThat(result.output).containsAll(setOf("1", "2", "3", "4"))
    }
}