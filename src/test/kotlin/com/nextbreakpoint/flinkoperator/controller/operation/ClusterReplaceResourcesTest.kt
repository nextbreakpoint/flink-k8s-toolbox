package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import io.kubernetes.client.models.V1JobBuilder
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1ServiceBuilder
import io.kubernetes.client.models.V1ServiceList
import io.kubernetes.client.models.V1StatefulSet
import io.kubernetes.client.models.V1StatefulSetBuilder
import io.kubernetes.client.models.V1StatefulSetList
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterReplaceResourcesTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = ClusterReplaceResources(flinkOptions, flinkClient, kubeClient)
    private val v1Job = V1JobBuilder().withNewMetadata().withName("test").endMetadata().build()
    private val v1Service = V1ServiceBuilder().withNewMetadata().withName("test").endMetadata().build()
    private val v1StatefulSet = V1StatefulSetBuilder().withNewMetadata().withName("test").endMetadata().build()
    private val resources = ClusterResources(
        bootstrapJob = v1Job,
        jobmanagerService = v1Service,
        jobmanagerStatefulSet = v1StatefulSet,
        taskmanagerStatefulSet = v1StatefulSet
    )

    @BeforeEach
    fun configure() {
        given(kubeClient.listJobManagerServices(eq(clusterId))).thenReturn(V1ServiceList().addItemsItem(mock(V1Service::class.java)))
        given(kubeClient.listJobManagerStatefulSets(eq(clusterId))).thenReturn(V1StatefulSetList().addItemsItem(mock(V1StatefulSet::class.java)))
        given(kubeClient.listTaskManagerStatefulSets(eq(clusterId))).thenReturn(V1StatefulSetList().addItemsItem(mock(V1StatefulSet::class.java)))
        given(kubeClient.createJobManagerService(eq(clusterId), any())).thenReturn(v1Service)
        given(kubeClient.replaceJobManagerStatefulSet(eq(clusterId), any())).thenReturn(v1StatefulSet)
        given(kubeClient.replaceTaskManagerStatefulSet(eq(clusterId), any())).thenReturn(v1StatefulSet)
    }

    @Test
    fun `should fail when job manager statefulset does not exists`() {
        given(kubeClient.listJobManagerStatefulSets(eq(clusterId))).thenReturn(V1StatefulSetList())
        val result = command.execute(clusterId, resources)
        verify(kubeClient, times(1)).listJobManagerStatefulSets(eq(clusterId))
        verify(kubeClient, times(1)).listTaskManagerStatefulSets(eq(clusterId))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when task manager statefulset does not exists`() {
        given(kubeClient.listTaskManagerStatefulSets(eq(clusterId))).thenReturn(V1StatefulSetList())
        val result = command.execute(clusterId, resources)
        verify(kubeClient, times(1)).listJobManagerStatefulSets(eq(clusterId))
        verify(kubeClient, times(1)).listTaskManagerStatefulSets(eq(clusterId))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should create job manager and replace task manager resources`() {
        val result = command.execute(clusterId, resources)
        verify(kubeClient, times(1)).deleteServices(eq(clusterId))
        verify(kubeClient, times(1)).listJobManagerStatefulSets(eq(clusterId))
        verify(kubeClient, times(1)).listTaskManagerStatefulSets(eq(clusterId))
        verify(kubeClient, times(1)).createJobManagerService(eq(clusterId), eq(resources))
        verify(kubeClient, times(1)).replaceJobManagerStatefulSet(eq(clusterId), eq(resources))
        verify(kubeClient, times(1)).replaceTaskManagerStatefulSet(eq(clusterId), eq(resources))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should create job manager and replace task manager resources when service does not exist`() {
        given(kubeClient.listJobManagerServices(eq(clusterId))).thenReturn(V1ServiceList())
        val result = command.execute(clusterId, resources)
        verify(kubeClient, times(1)).deleteServices(eq(clusterId))
        verify(kubeClient, times(1)).listJobManagerStatefulSets(eq(clusterId))
        verify(kubeClient, times(1)).listTaskManagerStatefulSets(eq(clusterId))
        verify(kubeClient, times(1)).createJobManagerService(eq(clusterId), eq(resources))
        verify(kubeClient, times(1)).replaceJobManagerStatefulSet(eq(clusterId), eq(resources))
        verify(kubeClient, times(1)).replaceTaskManagerStatefulSet(eq(clusterId), eq(resources))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.listJobManagerStatefulSets(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, resources)
        verify(kubeClient, times(1)).listJobManagerStatefulSets(eq(clusterId))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }
}