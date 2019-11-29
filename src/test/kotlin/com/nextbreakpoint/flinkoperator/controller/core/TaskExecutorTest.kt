package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
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

class TaskExecutorTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val controller = mock(OperationController::class.java)
    private val resources = mock(CachedResources::class.java)
    private val handler = mock(Task::class.java)
    private val taskHandlers = mapOf(ClusterTask.InitialiseCluster to handler)
    private val command = clusterUpdateStatus()

    private fun clusterUpdateStatus(): TaskExecutor {
        given(controller.flinkOptions).thenReturn(flinkOptions)
        given(controller.flinkClient).thenReturn(flinkClient)
        given(controller.kubeClient).thenReturn(kubeClient)
        return TaskExecutor(controller, taskHandlers)
    }

    @BeforeEach
    fun configure() {
        val actualBootstrapDigest = ClusterResource.computeDigest(cluster.spec?.bootstrap)
        val actualRuntimeDigest = ClusterResource.computeDigest(cluster.spec?.runtime)
        val actualJobManagerDigest = ClusterResource.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(cluster.spec?.taskManager)
        Status.setBootstrapDigest(cluster, actualBootstrapDigest)
        Status.setRuntimeDigest(cluster, actualRuntimeDigest)
        Status.setJobManagerDigest(cluster, actualJobManagerDigest)
        Status.setTaskManagerDigest(cluster, actualTaskManagerDigest)
        Status.setTaskAttempts(cluster, 1)
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        Status.setServiceMode(cluster, "Manual")
        Status.setSavepointMode(cluster, "Automatic")
        Status.setJobRestartPolicy(cluster, "Never")
    }

    @Test
    fun `should initialise cluster resource when task is not defined`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verify(controller, times(1)).updateStatus(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Executing)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Unknown)
        assertThat(Status.getTaskAttempts(cluster)).isEqualTo(0)
    }

    @Test
    fun `should fail when task handler is not defined`() {
        Status.appendTasks(cluster, listOf(ClusterTask.ClusterRunning))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(controller)
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `should update cluster resource when task status is executing and result status is success`() {
        given(handler.onExecuting(any())).thenReturn(TaskResult(action = TaskAction.NEXT, output = ""))
        Status.setTaskStatus(cluster, TaskStatus.Executing)
        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verify(controller, times(1)).updateStatus(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onExecuting(any())
        verifyNoMoreInteractions(handler)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Awaiting)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is executing and result status is failed`() {
        given(handler.onExecuting(any())).thenReturn(TaskResult(action = TaskAction.FAIL, output = ""))
        Status.setTaskStatus(cluster, TaskStatus.Executing)
        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verify(controller, times(1)).updateStatus(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onExecuting(any())
        verifyNoMoreInteractions(handler)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Failed)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is executing and result status is await`() {
        given(handler.onExecuting(any())).thenReturn(TaskResult(action = TaskAction.REPEAT, output = ""))
        Status.setTaskStatus(cluster, TaskStatus.Executing)
        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onExecuting(any())
        verifyNoMoreInteractions(handler)
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Executing)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is awaiting and result status is success`() {
        given(handler.onAwaiting(any())).thenReturn(TaskResult(action = TaskAction.NEXT, output = ""))
        Status.setTaskStatus(cluster, TaskStatus.Awaiting)
        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verify(controller, times(1)).updateStatus(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onAwaiting(any())
        verifyNoMoreInteractions(handler)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Idle)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is awaiting and result status is failed`() {
        given(handler.onAwaiting(any())).thenReturn(TaskResult(action = TaskAction.FAIL, output = ""))
        Status.setTaskStatus(cluster, TaskStatus.Awaiting)
        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verify(controller, times(1)).updateStatus(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onAwaiting(any())
        verifyNoMoreInteractions(handler)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Failed)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is awaiting and result status is await`() {
        given(handler.onAwaiting(any())).thenReturn(TaskResult(action = TaskAction.REPEAT, output = ""))
        Status.setTaskStatus(cluster, TaskStatus.Awaiting)
        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onAwaiting(any())
        verifyNoMoreInteractions(handler)
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Awaiting)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is idle and there is a new task`() {
        given(handler.onIdle(any())).thenReturn(TaskResult(action = TaskAction.NEXT, output = ""))
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster, ClusterTask.ClusterRunning))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verify(controller, times(1)).updateStatus(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Executing)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should not update cluster resource when task status is idle and there isn't a new task`() {
        given(handler.onIdle(any())).thenReturn(TaskResult(action = TaskAction.NEXT, output = ""))
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Idle)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is idle and result status is failed`() {
        given(handler.onIdle(any())).thenReturn(TaskResult(action = TaskAction.FAIL, output = ""))
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster, ClusterTask.ClusterRunning))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verify(controller, times(1)).updateStatus(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onIdle(any())
        verifyNoMoreInteractions(handler)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.InitialiseCluster)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Failed)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update cluster resource when task status is failed`() {
        given(handler.onFailed(any())).thenReturn(TaskResult(action = TaskAction.NEXT, output = ""))
        Status.setTaskStatus(cluster, TaskStatus.Failed)
        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster, ClusterTask.ClusterRunning))
        val timestamp = Status.getOperatorTimestamp(cluster)
        command.update(clusterId, cluster, resources)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verify(controller, times(1)).updateStatus(eq(clusterId), any())
        verifyNoMoreInteractions(controller)
        verify(handler, times(1)).onFailed(any())
        verifyNoMoreInteractions(handler)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
        assertThat(Status.getCurrentTaskStatus(cluster)).isEqualTo(TaskStatus.Executing)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Failed)
    }
}