package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import io.kubernetes.client.models.V1ServiceBuilder
import io.kubernetes.client.models.V1ServiceList
import io.kubernetes.client.models.V1StatefulSetBuilder
import io.kubernetes.client.models.V1StatefulSetList
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterCreateResourcesTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = ClusterCreateResources(flinkOptions, flinkClient, kubeClient)
    private val v1Service = V1ServiceBuilder().withNewMetadata().withName("test").endMetadata().build()
    private val v1StatefulSet = V1StatefulSetBuilder().withNewMetadata().withName("test").endMetadata().build()
    private val resources = ClusterResources(
        jobmanagerService = v1Service,
        jobmanagerStatefulSet = v1StatefulSet,
        taskmanagerStatefulSet = v1StatefulSet
    )

    @BeforeEach
    fun configure() {
        given(kubeClient.createJobManagerService(eq(clusterId), any())).thenReturn(v1Service)
        given(kubeClient.createJobManagerStatefulSet(eq(clusterId), any())).thenReturn(v1StatefulSet)
        given(kubeClient.createTaskManagerStatefulSet(eq(clusterId), any())).thenReturn(v1StatefulSet)
    }

    @Test
    fun `should create job manager and task manager resources`() {
        val result = command.execute(clusterId, resources)
        verify(kubeClient, times(1)).createJobManagerService(eq(clusterId), eq(resources))
        verify(kubeClient, times(1)).createJobManagerStatefulSet(eq(clusterId), eq(resources))
        verify(kubeClient, times(1)).createTaskManagerStatefulSet(eq(clusterId), eq(resources))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.createJobManagerService(eq(clusterId), any())).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, resources)
        verify(kubeClient, times(1)).createJobManagerService(eq(clusterId), any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isNull()
    }
}