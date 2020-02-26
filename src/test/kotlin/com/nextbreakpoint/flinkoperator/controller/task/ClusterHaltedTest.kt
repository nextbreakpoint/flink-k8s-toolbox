package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskAction
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito
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

class ClusterHaltedTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val context = mock(TaskContext::class.java)
    private val clusterScaling = ClusterScaling(taskManagers = 1, taskSlots = 1)
    private val task = ClusterHalted()

    @BeforeEach
    fun configure() {
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(0L)
        given(context.timeSinceLastSavepointRequestInSeconds()).thenReturn(0L)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, false))
        val actualBootstrapDigest = ClusterResource.computeDigest(cluster.spec?.bootstrap)
        val actualRuntimeDigest = ClusterResource.computeDigest(cluster.spec?.runtime)
        val actualJobManagerDigest = ClusterResource.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(cluster.spec?.taskManager)
        Status.setBootstrapDigest(cluster, actualBootstrapDigest)
        Status.setRuntimeDigest(cluster, actualRuntimeDigest)
        Status.setJobManagerDigest(cluster, actualJobManagerDigest)
        Status.setTaskManagerDigest(cluster, actualTaskManagerDigest)
        Status.appendTasks(cluster, listOf(ClusterTask.ClusterHalted))
        Status.setTaskAttempts(cluster, 1)
        Status.setTaskManagers(cluster, 1)
        Status.setTaskSlots(cluster, 1)
        Status.setBootstrap(cluster, cluster.spec.bootstrap)
        Status.setServiceMode(cluster, "Manual")
        Status.setSavepointMode(cluster, "Automatic")
        Status.setJobRestartPolicy(cluster, "Always")
    }

    @Test
    fun `onExecuting should return expected result`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.SKIP)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getTaskAttempts(cluster)).isEqualTo(0)
    }

    @Test
    fun `onAwaiting should return expected result`() {
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onIdle should do nothing when cluster status is suspended and digests didn't changed`() {
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isClusterTerminated(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(500)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).isClusterTerminated(eq(clusterId))
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when cluster status is failed and digests didn't changed`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        val timestamp = Status.getOperatorTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, times(1)).isClusterRunning(eq(clusterId))
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink image digest changed but status prevents restart`() {
        Status.setRuntimeDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink job digest changed but status prevents restart`() {
        Status.setBootstrapDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and flink job digest changed`() {
        Status.setBootstrapDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and flink image digest changed`() {
        Status.setRuntimeDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and job manager digest changed`() {
        Status.setJobManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and task manager digest changed`() {
        Status.setTaskManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should check if cluster is running when cluster status is failed`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, false))
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, times(1)).isClusterRunning(eq(clusterId))
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should change status when cluster status is failed but cluster is running`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, false))
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, times(1)).isClusterRunning(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
        assertThat(Status.getNextOperatorTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should change status when cluster status is failed and cluster is running but job is finished`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, true))
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, times(1)).isClusterRunning(eq(clusterId))
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
        assertThat(Status.getNextOperatorTask(cluster)).isNull()
    }

    @Test
    fun `onIdle should not restart job when cluster status is failed and cluster is not ready`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        Status.setTaskAttempts(cluster, 3)
        val timestamp = Status.getOperatorTimestamp(cluster)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, false))
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, times(1)).isClusterRunning(eq(clusterId))
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(Status.getTaskAttempts(cluster)).isEqualTo(0)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should restart job when cluster status is failed but cluster is ready after 3 attempts`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        Status.setTaskAttempts(cluster, 3)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, false))
        given(context.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, times(1)).isClusterRunning(eq(clusterId))
        verify(context, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getNextOperatorTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getNextOperatorTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should not change status when cluster status is suspended and cluster is terminated`() {
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        Status.setActiveTaskManagers(cluster, 1)
        val timestamp = Status.getOperatorTimestamp(cluster)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        given(context.isClusterTerminated(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(500)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).isClusterTerminated(eq(clusterId))
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `onIdle should not change status when cluster status is suspended and there aren't active task managers`() {
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        Status.setActiveTaskManagers(cluster, 0)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(500)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `onIdle should change status when cluster status is suspended and cluster is not terminated`() {
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        Status.setActiveTaskManagers(cluster, 1)
        val timestamp = Status.getOperatorTimestamp(cluster)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        given(context.isClusterTerminated(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(500)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).isClusterTerminated(eq(clusterId))
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.SuspendCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `onIdle should not change status when cluster status is terminated and cluster is terminated`() {
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        Status.setActiveTaskManagers(cluster, 1)
        val timestamp = Status.getOperatorTimestamp(cluster)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        given(context.isClusterTerminated(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, null))
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(500)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).isClusterTerminated(eq(clusterId))
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `onIdle should not change status when cluster status is terminated and there aren't active task managers`() {
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        Status.setActiveTaskManagers(cluster, 0)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(500)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `onIdle should change status when cluster status is terminated and cluster is not terminated`() {
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        Status.setActiveTaskManagers(cluster, 1)
        val timestamp = Status.getOperatorTimestamp(cluster)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        given(context.isClusterTerminated(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, null))
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(500)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).isClusterTerminated(eq(clusterId))
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatedCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `onIdle should start cluster when there is a manual action`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        Annotations.setManualAction(cluster, ManualAction.START)
        val tasks = listOf(ClusterTask.StartJob, ClusterTask.ClusterRunning)
        given(context.startCluster(eq(clusterId), KotlinMockito.any())).thenReturn(OperationResult(OperationStatus.COMPLETED, tasks))
        val actionTimestamp = Annotations.getActionTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        val options = StartOptions(withoutSavepoint = false)
        verify(context, times(1)).startCluster(eq(clusterId), eq(options))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isEqualTo("[name=test] Starting cluster...")
        assertThat(actionTimestamp).isNotEqualTo(Annotations.getActionTimestamp(cluster))
        assertThat(Annotations.getManualAction(cluster)).isEqualTo(ManualAction.NONE)
    }

    @Test
    fun `onIdle should stop cluster when there is a manual action`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        Annotations.setManualAction(cluster, ManualAction.STOP)
        val tasks = listOf(ClusterTask.StartJob, ClusterTask.ClusterRunning)
        given(context.stopCluster(eq(clusterId), KotlinMockito.any())).thenReturn(OperationResult(OperationStatus.COMPLETED, tasks))
        val actionTimestamp = Annotations.getActionTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        val options = StopOptions(withoutSavepoint = false, deleteResources = false)
        verify(context, times(1)).stopCluster(eq(clusterId), eq(options))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isEqualTo("[name=test] Stopping cluster...")
        assertThat(actionTimestamp).isNotEqualTo(Annotations.getActionTimestamp(cluster))
        assertThat(Annotations.getManualAction(cluster)).isEqualTo(ManualAction.NONE)
    }

    @Test
    fun `onIdle should restart job when cluster status is suspended and flink job digest changed`() {
        Status.setBootstrapDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.RefreshStatus)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.SuspendCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `onIdle should restart job when cluster status is suspended and flink image digest changed`() {
        Status.setRuntimeDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatedCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `onIdle should restart job when cluster status is suspended and job manager digest changed`() {
        Status.setJobManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatedCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `onIdle should restart job when cluster status is suspended and task manager digest changed`() {
        Status.setTaskManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatedCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
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