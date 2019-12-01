package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskAction
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class CreateResourcesTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val context = mock(TaskContext::class.java)
    private val resources = mock(CachedResources::class.java)
    private val clusterScaling = ClusterScaling(taskManagers = 1, taskSlots = 1)
    private val task = CreateResources()

    @BeforeEach
    fun configure() {
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        given(context.resources).thenReturn(resources)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(0)
        val actualBootstrapDigest = ClusterResource.computeDigest(cluster.spec?.bootstrap)
        val actualRuntimeDigest = ClusterResource.computeDigest(cluster.spec?.runtime)
        val actualJobManagerDigest = ClusterResource.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(cluster.spec?.taskManager)
        Status.setBootstrapDigest(cluster, actualBootstrapDigest)
        Status.setRuntimeDigest(cluster, actualRuntimeDigest)
        Status.setJobManagerDigest(cluster, actualJobManagerDigest)
        Status.setTaskManagerDigest(cluster, actualTaskManagerDigest)
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
        assertThat(result.action).isEqualTo(TaskAction.FAIL)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have been already created and cluster is ready`() {
        val resources = TestFactory.createResourcesWithoutJob(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.SKIP)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources already exists but they have changed`() {
        Status.setJobManagerDigest(cluster, "xxxxxxx")
        val resources = TestFactory.createResourcesWithoutJob(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        given(context.createClusterResources(eq(clusterId), any())).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verify(context, times(1)).createClusterResources(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have not been created yet`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        given(context.createClusterResources(eq(clusterId), any())).thenReturn(OperationResult(OperationStatus.RETRY, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verify(context, times(1)).createClusterResources(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.REPEAT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources can't be created`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        given(context.createClusterResources(eq(clusterId), any())).thenReturn(OperationResult(OperationStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verify(context, times(1)).createClusterResources(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.REPEAT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have been created`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        given(context.createClusterResources(eq(clusterId), any())).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verify(context, times(1)).createClusterResources(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
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
        assertThat(result.action).isEqualTo(TaskAction.FAIL)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when resources are not ready`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.REPEAT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster is not ready yet`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.REPEAT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster has failed`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.FAILED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.REPEAT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster is ready`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onIdle should return expected result`() {
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onFailed should return expected result`() {
        val result = task.onFailed(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.REPEAT)
        assertThat(result.output).isNotNull()
    }
}