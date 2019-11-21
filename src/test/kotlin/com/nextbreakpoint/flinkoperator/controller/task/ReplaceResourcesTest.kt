package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import io.kubernetes.client.models.V1StatefulSetBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ReplaceResourcesTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val context = mock(TaskContext::class.java)
    private val resources = mock(CachedResources::class.java)
    private val cache = mock(Cache::class.java)
    private val clusterScaling = ClusterScaling(taskManagers = 1, taskSlots = 1)
    private val task = ReplaceResources()

    @BeforeEach
    fun configure() {
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        given(context.resources).thenReturn(resources)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(0)
        given(cache.getResources()).thenReturn(resources)
        val jobmanagerStatefulSets = mapOf(clusterId to V1StatefulSetBuilder().withNewMetadata().endMetadata().build())
        val taskmanagerStatefulSets = mapOf(clusterId to V1StatefulSetBuilder().withNewMetadata().endMetadata().build())
        given(resources.jobmanagerStatefulSets).thenReturn(jobmanagerStatefulSets)
        given(resources.taskmanagerStatefulSets).thenReturn(taskmanagerStatefulSets)
        Status.setTaskManagers(cluster, 1)
        Status.setTaskSlots(cluster, 1)
    }

    @Test
    fun `onExecuting should return expected result when operation times out`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.CREATING_CLUSTER_TIMEOUT + 1)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have been already created and cluster is ready`() {
        val resources = TestFactory.createResourcesWithoutJob(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have not been created yet`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(context.replaceClusterResources(eq(clusterId), any())).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verify(context, times(1)).replaceClusterResources(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources can't be created`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(context.replaceClusterResources(eq(clusterId), any())).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verify(context, times(1)).replaceClusterResources(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have been created`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(context.replaceClusterResources(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verify(context, times(1)).replaceClusterResources(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when operation times out`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.CREATING_CLUSTER_TIMEOUT + 1)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when resources are not ready`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster is not ready yet`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster has failed`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster is ready`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onIdle should return expected result`() {
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onFailed should return expected result`() {
        val result = task.onFailed(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }
}