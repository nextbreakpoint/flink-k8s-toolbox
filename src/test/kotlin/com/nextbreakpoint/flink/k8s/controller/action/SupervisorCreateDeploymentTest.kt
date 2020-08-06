package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito.any
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1DeploymentBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class SupervisorCreateDeploymentTest {
    private val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "123")
    private val deployment = mock(V1Deployment::class.java)
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = SupervisorCreateDeployment(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        val deployment = V1DeploymentBuilder().withNewMetadata().withName("xxx").endMetadata().build()
        given(kubeClient.createSupervisorDeployment(eq(clusterSelector), any())).thenReturn(deployment)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.createSupervisorDeployment(eq(clusterSelector), any())).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, deployment)
        verify(kubeClient, times(1)).createSupervisorDeployment(eq(clusterSelector), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should create supervisor deployment`() {
        val result = command.execute(clusterSelector, deployment)
        verify(kubeClient, times(1)).createSupervisorDeployment(eq(clusterSelector), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isEqualTo("xxx")
    }
}