package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
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

class ClusterCreateResourcesTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val command = ClusterCreateResources(flinkOptions, flinkContext, kubernetesContext)
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
        given(kubernetesContext.listJobManagerServices(eq(clusterId))).thenReturn(V1ServiceList())
        given(kubernetesContext.listJobManagerStatefulSets(eq(clusterId))).thenReturn(V1StatefulSetList())
        given(kubernetesContext.listTaskManagerStatefulSets(eq(clusterId))).thenReturn(V1StatefulSetList())
        given(kubernetesContext.createJobManagerService(eq(clusterId), any())).thenReturn(v1Service)
        given(kubernetesContext.createJobManagerStatefulSet(eq(clusterId), any())).thenReturn(v1StatefulSet)
        given(kubernetesContext.createTaskManagerStatefulSet(eq(clusterId), any())).thenReturn(v1StatefulSet)
    }

    @Test
    fun `should fail when job manager service already exists`() {
        given(kubernetesContext.listJobManagerServices(eq(clusterId))).thenReturn(V1ServiceList().addItemsItem(mock(V1Service::class.java)))
        val result = command.execute(clusterId, resources)
        verify(kubernetesContext, times(1)).listJobManagerServices(eq(clusterId))
        verify(kubernetesContext, times(1)).listJobManagerStatefulSets(eq(clusterId))
        verify(kubernetesContext, times(1)).listTaskManagerStatefulSets(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when job manager statefulset already exists`() {
        given(kubernetesContext.listJobManagerStatefulSets(eq(clusterId))).thenReturn(V1StatefulSetList().addItemsItem(mock(V1StatefulSet::class.java)))
        val result = command.execute(clusterId, resources)
        verify(kubernetesContext, times(1)).listJobManagerServices(eq(clusterId))
        verify(kubernetesContext, times(1)).listJobManagerStatefulSets(eq(clusterId))
        verify(kubernetesContext, times(1)).listTaskManagerStatefulSets(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when task manager statefulset already exists`() {
        given(kubernetesContext.listTaskManagerStatefulSets(eq(clusterId))).thenReturn(V1StatefulSetList().addItemsItem(mock(V1StatefulSet::class.java)))
        val result = command.execute(clusterId, resources)
        verify(kubernetesContext, times(1)).listJobManagerServices(eq(clusterId))
        verify(kubernetesContext, times(1)).listJobManagerStatefulSets(eq(clusterId))
        verify(kubernetesContext, times(1)).listTaskManagerStatefulSets(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should create job manager and task manager resources`() {
        val result = command.execute(clusterId, resources)
        verify(kubernetesContext, times(1)).listJobManagerServices(eq(clusterId))
        verify(kubernetesContext, times(1)).listJobManagerStatefulSets(eq(clusterId))
        verify(kubernetesContext, times(1)).listTaskManagerStatefulSets(eq(clusterId))
        verify(kubernetesContext, times(1)).createJobManagerService(eq(clusterId), eq(resources))
        verify(kubernetesContext, times(1)).createJobManagerStatefulSet(eq(clusterId), eq(resources))
        verify(kubernetesContext, times(1)).createTaskManagerStatefulSet(eq(clusterId), eq(resources))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when kubernetesContext throws exception`() {
        given(kubernetesContext.listJobManagerServices(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, resources)
        verify(kubernetesContext, times(1)).listJobManagerServices(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }
}