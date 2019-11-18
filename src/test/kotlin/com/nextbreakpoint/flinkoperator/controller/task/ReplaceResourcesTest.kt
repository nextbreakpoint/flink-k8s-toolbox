package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.Status
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
    private val controller = mock(OperationController::class.java)
    private val resources = mock(CachedResources::class.java)
    private val cache = mock(Cache::class.java)
    private val clusterScaling = ClusterScaling(taskManagers = 1, taskSlots = 1)
    private val time = System.currentTimeMillis()
    private val task = ReplaceResources()

    @BeforeEach
    fun configure() {
        given(context.operatorTimestamp).thenReturn(time)
        given(context.controller).thenReturn(controller)
        given(context.resources).thenReturn(resources)
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        given(controller.cache).thenReturn(cache)
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
        given(controller.currentTimeMillis()).thenReturn(time + Timeout.CREATING_CLUSTER_TIMEOUT + 1)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have been already created and cluster is ready`() {
        val resources = TestFactory.createResourcesWithoutJob(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have not been created yet`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.replaceClusterResources(eq(clusterId), any())).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verify(controller, times(1)).replaceClusterResources(eq(clusterId), any())
        verify(controller, atLeastOnce()).cache
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources can't be created`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.replaceClusterResources(eq(clusterId), any())).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verify(controller, times(1)).replaceClusterResources(eq(clusterId), any())
        verify(controller, atLeastOnce()).cache
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have been created`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.replaceClusterResources(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verify(controller, times(1)).replaceClusterResources(eq(clusterId), any())
        verify(controller, atLeastOnce()).cache
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when operation times out`() {
        given(controller.currentTimeMillis()).thenReturn(time + Timeout.CREATING_CLUSTER_TIMEOUT + 1)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when resources are not ready`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster is not ready yet`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster has failed`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster is ready`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onIdle should return expected result`() {
        val result = task.onIdle(context)
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onFailed should return expected result`() {
        val result = task.onFailed(context)
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }
}