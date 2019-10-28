package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterUpdateStatusTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster("test", "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val controller = mock(OperatorController::class.java)
    private val resources = mock(OperatorResources::class.java)
    private val handler = mock(OperatorTaskHandler::class.java)
    private val operatorTasks = mapOf(OperatorTask.INITIALISE_CLUSTER to handler)
    private val command = clusterUpdateStatus()

    private fun clusterUpdateStatus(): ClusterUpdateStatus {
        given(controller.flinkOptions).thenReturn(flinkOptions)
        given(controller.flinkContext).thenReturn(flinkContext)
        given(controller.kubernetesContext).thenReturn(kubernetesContext)
        return ClusterUpdateStatus(controller, resources, operatorTasks)
    }

    @BeforeEach
    fun configure() {
        val actualFlinkJobDigest = CustomResources.computeDigest(cluster.spec?.flinkJob)
        val actualFlinkImageDigest = CustomResources.computeDigest(cluster.spec?.flinkImage)
        val actualJobManagerDigest = CustomResources.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = CustomResources.computeDigest(cluster.spec?.taskManager)
        OperatorAnnotations.setFlinkJobDigest(cluster, actualFlinkJobDigest)
        OperatorAnnotations.setFlinkImageDigest(cluster, actualFlinkImageDigest)
        OperatorAnnotations.setJobManagerDigest(cluster, actualJobManagerDigest)
        OperatorAnnotations.setTaskManagerDigest(cluster, actualTaskManagerDigest)
        OperatorAnnotations.setOperatorTaskAttempts(cluster, 1)
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        verify(controller, times(1)).flinkOptions
        verify(controller, times(1)).flinkContext
        verify(controller, times(1)).kubernetesContext
    }

    @Test
    fun `should fail when task handler is not defined`() {
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.CLUSTER_RUNNING))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
        assertThat(timestamp).isEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
    }

    @Test
    fun `should initialise cluster resource when task is not defined`() {
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }

    @Test
    fun `should update cluster resource when task status is executing and result status is success`() {
        given(handler.onExecuting(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.EXECUTING)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onExecuting(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.AWAITING)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }

    @Test
    fun `should update cluster resource when task status is executing and result status is failed`() {
        given(handler.onExecuting(any())).thenReturn(Result(status = ResultStatus.FAILED, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.EXECUTING)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onExecuting(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.FAILED)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }

    @Test
    fun `should update cluster resource when task status is executing and result status is await`() {
        given(handler.onExecuting(any())).thenReturn(Result(status = ResultStatus.AWAIT, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.EXECUTING)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onExecuting(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
        assertThat(timestamp).isEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.EXECUTING)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }

    @Test
    fun `should update cluster resource when task status is awaiting and result status is success`() {
        given(handler.onAwaiting(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.AWAITING)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onAwaiting(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.IDLE)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }

    @Test
    fun `should update cluster resource when task status is awaiting and result status is failed`() {
        given(handler.onAwaiting(any())).thenReturn(Result(status = ResultStatus.FAILED, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.AWAITING)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onAwaiting(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.FAILED)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }

    @Test
    fun `should update cluster resource when task status is awaiting and result status is await`() {
        given(handler.onAwaiting(any())).thenReturn(Result(status = ResultStatus.AWAIT, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.AWAITING)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onAwaiting(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
        assertThat(timestamp).isEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.AWAITING)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }

    @Test
    fun `should update cluster resource when task status is idle and there is a new task`() {
        given(handler.onIdle(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.IDLE)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER, OperatorTask.CLUSTER_RUNNING))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.EXECUTING)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }

    @Test
    fun `should not update cluster resource when task status is idle and there isn't a new task`() {
        given(handler.onIdle(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.IDLE)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
        assertThat(timestamp).isEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.IDLE)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }

    @Test
    fun `should update cluster resource when task status is idle and result status is failed`() {
        given(handler.onIdle(any())).thenReturn(Result(status = ResultStatus.FAILED, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.IDLE)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER, OperatorTask.CLUSTER_RUNNING))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.FAILED)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }

    @Test
    fun `should update cluster resource when task status is failed`() {
        given(handler.onFailed(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.FAILED)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER, OperatorTask.CLUSTER_RUNNING))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, times(1)).updateAnnotations(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onFailed(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_HALTED)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.EXECUTING)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.FAILED)
    }

    @Test
    fun `should update cluster resource when savepoint path is changed`() {
        cluster.spec.flinkOperator.savepointPath = "file://tmp/000"
        given(handler.onIdle(any())).thenReturn(Result(status = ResultStatus.SUCCESS, output = ""))
        OperatorAnnotations.setTaskStatus(cluster, TaskStatus.IDLE)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = command.execute(clusterId, cluster)
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verify(controller, times(1)).updateSavepoint(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
        assertThat(timestamp).isEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.IDLE)
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
    }
}