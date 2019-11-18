package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorTask
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

class UpdateStatusTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val controller = mock(OperatorController::class.java)
    private val resources = mock(OperatorResources::class.java)
    private val handler = mock(OperatorTask::class.java)
    private val cache = mock(OperatorCache::class.java)
    private val taskHandlers = mapOf(ClusterTask.InitialiseCluster to handler)
    private val command = clusterUpdateStatus()

    private fun clusterUpdateStatus(): UpdateStatus {
        given(controller.flinkOptions).thenReturn(flinkOptions)
        given(controller.flinkContext).thenReturn(flinkContext)
        given(controller.kubernetesContext).thenReturn(kubernetesContext)
        given(controller.taskHandlers).thenReturn(taskHandlers)
        given(controller.cache).thenReturn(cache)
        return UpdateStatus(controller)
    }

    @BeforeEach
    fun configure() {
        val actualBootstrapDigest = CustomResources.computeDigest(cluster.spec?.bootstrap)
        val actualRuntimeDigest = CustomResources.computeDigest(cluster.spec?.runtime)
        val actualJobManagerDigest = CustomResources.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = CustomResources.computeDigest(cluster.spec?.taskManager)
        OperatorState.setBootstrapDigest(cluster, actualBootstrapDigest)
        OperatorState.setRuntimeDigest(cluster, actualRuntimeDigest)
        OperatorState.setJobManagerDigest(cluster, actualJobManagerDigest)
        OperatorState.setTaskManagerDigest(cluster, actualTaskManagerDigest)
        OperatorState.setTaskAttempts(cluster, 1)
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        verify(controller, times(1)).flinkOptions
        verify(controller, times(1)).flinkContext
        verify(controller, times(1)).kubernetesContext
        given(cache.getFlinkCluster(eq(clusterId))).thenReturn(cluster)
        given(cache.getResources()).thenReturn(resources)
    }

    @Test
    fun `should initialise cluster resource when task is not defined`() {
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).updateState(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Executing)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Unknown)
        assertThat(OperatorState.getTaskAttempts(cluster)).isEqualTo(0)
    }

    @Test
    fun `should fail when task handler is not defined`() {
        OperatorState.appendTasks(cluster, listOf(ClusterTask.ClusterRunning))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verifyNoMoreInteractions(controller)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `should update cluster resource when task status is executing and result status is success`() {
        given(handler.onExecuting(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Executing)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verify(controller, times(1)).updateState(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onExecuting(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Awaiting)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is executing and result status is failed`() {
        given(handler.onExecuting(any())).thenReturn(Result(status = ResultStatus.FAILED, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Executing)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verify(controller, times(1)).updateState(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onExecuting(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Failed)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is executing and result status is await`() {
        given(handler.onExecuting(any())).thenReturn(Result(status = ResultStatus.AWAIT, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Executing)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onExecuting(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Executing)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is awaiting and result status is success`() {
        given(handler.onAwaiting(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Awaiting)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verify(controller, times(1)).updateState(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onAwaiting(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Idle)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is awaiting and result status is failed`() {
        given(handler.onAwaiting(any())).thenReturn(Result(status = ResultStatus.FAILED, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Awaiting)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verify(controller, times(1)).updateState(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onAwaiting(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Failed)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is awaiting and result status is await`() {
        given(handler.onAwaiting(any())).thenReturn(Result(status = ResultStatus.AWAIT, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Awaiting)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onAwaiting(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Awaiting)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is idle and there is a new task`() {
        given(handler.onIdle(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Idle)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster, ClusterTask.ClusterRunning))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verify(controller, times(1)).updateState(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Executing)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should not update cluster resource when task status is idle and there isn't a new task`() {
        given(handler.onIdle(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Idle)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Idle)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is idle and result status is failed`() {
        given(handler.onIdle(any())).thenReturn(Result(status = ResultStatus.FAILED, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Idle)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster, ClusterTask.ClusterRunning))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verify(controller, times(1)).updateState(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Failed)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is failed`() {
        given(handler.onFailed(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Failed)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster, ClusterTask.ClusterRunning))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verify(controller, times(1)).updateState(eq(clusterId), any())
        verify(controller, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onFailed(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Executing)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Failed)
        assertThat(OperatorAnnotations.getManualAction(cluster)).isEqualTo(ManualAction.NONE)
    }

    @Test
    fun `should update cluster resource when savepoint path is changed`() {
        cluster.spec.operator.savepointPath = "file://tmp/000"
        given(handler.onIdle(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorState.setTaskStatus(cluster, TaskStatus.Idle)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, atLeastOnce()).cache
        verify(controller, times(1)).taskHandlers
        verify(controller, times(1)).updateSavepoint(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        verify(cache, times(1)).getResources()
        verify(cache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(cache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(OperatorState.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Idle)
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }
}