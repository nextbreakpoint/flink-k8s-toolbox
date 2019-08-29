package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.controller.OperatorTimeouts
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
    private val cluster = TestFactory.aCluster("test", "flink")
    private val context = mock(OperatorContext::class.java)
    private val controller = mock(OperatorController::class.java)
    private val resources = mock(OperatorResources::class.java)
    private val time = System.currentTimeMillis()
    private val task = CreateResources()

    @BeforeEach
    fun configure() {
        given(context.lastUpdated).thenReturn(time)
        given(context.controller).thenReturn(controller)
        given(context.resources).thenReturn(resources)
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        given(context.haveClusterResourcesDiverged(any())).thenReturn(false)
    }

    @Test
    fun `onExecuting should return expected result when operation times out`() {
        given(controller.currentTimeMillis()).thenReturn(time + OperatorTimeouts.CREATING_CLUSTER_TIMEOUT + 1)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
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
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have not been created yet`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.createClusterResources(eq(clusterId), any())).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId))
        verify(controller, times(1)).createClusterResources(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources can't be created`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.createClusterResources(eq(clusterId), any())).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId))
        verify(controller, times(1)).createClusterResources(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when resources have been created`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.createClusterResources(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId))
        verify(controller, times(1)).createClusterResources(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when operation times out`() {
        given(controller.currentTimeMillis()).thenReturn(time + OperatorTimeouts.CREATING_CLUSTER_TIMEOUT + 1)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when resources have not been created yet`() {
        given(context.haveClusterResourcesDiverged(any())).thenReturn(true)
        val resources = TestFactory.createEmptyResources()
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when resources are not ready`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(context.haveClusterResourcesDiverged(any())).thenReturn(true)
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster is not ready yet`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster has failed`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when cluster is ready`() {
        val resources = TestFactory.createResources(clusterId.uuid, cluster)
        given(context.resources).thenReturn(resources)
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).haveClusterResourcesDiverged(any())
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterReady(eq(clusterId))
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