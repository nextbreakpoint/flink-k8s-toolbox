package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskAction
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
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

class ClusterRunningTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val context = mock(TaskContext::class.java)
    private val resources = mock(CachedResources::class.java)
    private val task = ClusterRunning()

    @BeforeEach
    fun configure() {
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        given(context.resources).thenReturn(resources)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(0L)
        given(context.timeSinceLastSavepointRequestInSeconds()).thenReturn(0L)
        val actualBootstrapDigest = ClusterResource.computeDigest(cluster.spec?.bootstrap)
        val actualRuntimeDigest = ClusterResource.computeDigest(cluster.spec?.runtime)
        val actualJobManagerDigest = ClusterResource.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(cluster.spec?.taskManager)
        Status.setBootstrapDigest(cluster, actualBootstrapDigest)
        Status.setRuntimeDigest(cluster, actualRuntimeDigest)
        Status.setJobManagerDigest(cluster, actualJobManagerDigest)
        Status.setTaskManagerDigest(cluster, actualTaskManagerDigest)
        Status.appendTasks(cluster, listOf(ClusterTask.ClusterRunning))
        Status.setTaskManagers(cluster, 1)
        Status.setBootstrap(cluster, cluster.spec.bootstrap)
        Status.setServiceMode(cluster, "Manual")
        Status.setSavepointMode(cluster, "Automatic")
        Status.setJobRestartPolicy(cluster, "Always")
        val taskmanagersStatefuleset = V1StatefulSetBuilder()
            .withNewSpec()
            .withReplicas(1)
            .endSpec()
            .withNewStatus()
            .withReplicas(1)
            .withReadyReplicas(1)
            .endStatus()
            .build()
        given(resources.taskmanagerStatefulSets).thenReturn(mapOf(clusterId to taskmanagersStatefuleset))
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
        assertThat(Status.getNextOperatorTask(cluster)).isNull()
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
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
    fun `onIdle should do nothing when digests didn't changed`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, false))
        val timestamp = Status.getOperatorTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        verify(context, times(2)).isClusterRunning(eq(clusterId))
        verify(context, times(1)).timeSinceLastSavepointRequestInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink image digest changed but status prevents restart`() {
        Status.setRuntimeDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).timeSinceLastSavepointRequestInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink job digest changed but status prevents restart`() {
        Status.setBootstrapDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        verify(context, times(1)).timeSinceLastSavepointRequestInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should restart cluster when flink image digest changed`() {
        System.setProperty("disableReplaceStrategy", "true")
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
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CancelJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
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
    fun `onIdle should restart cluster when flink image digest changed and replace strategy is enabled`() {
        System.setProperty("disableReplaceStrategy", "false")
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
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CancelJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart cluster when job manager digest changed`() {
        System.setProperty("disableReplaceStrategy", "true")
        Status.setJobManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Running)
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
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CancelJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
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
    fun `onIdle should restart cluster when job manager digest changed and replace strategy is eanbled`() {
        System.setProperty("disableReplaceStrategy", "false")
        Status.setJobManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Running)
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
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CancelJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart cluster when task manager digest changed`() {
        System.setProperty("disableReplaceStrategy", "true")
        Status.setTaskManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Running)
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
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CancelJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
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
    fun `onIdle should restart cluster when task manager digest changed and replace strategy is eanbled`() {
        System.setProperty("disableReplaceStrategy", "false")
        Status.setTaskManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Running)
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
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CancelJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when job digest changed`() {
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
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CancelJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.RefreshStatus)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should update attempts if cluster is running`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        Status.setTaskAttempts(cluster, 2)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, false))
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        verify(context, times(2)).isClusterRunning(eq(clusterId))
        verify(context, times(1)).timeSinceLastSavepointRequestInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getTaskAttempts(cluster)).isEqualTo(0)
    }

    @Test
    fun `onIdle should increment attempts when cluster is not running`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        Status.setTaskAttempts(cluster, 2)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, false))
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        verify(context, times(2)).isClusterRunning(eq(clusterId))
        verify(context, times(1)).timeSinceLastSavepointRequestInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getTaskAttempts(cluster)).isEqualTo(3)
    }

    @Test
    fun `onIdle should change status when cluster is not running after 3 attempts`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        Status.setTaskAttempts(cluster, 3)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.RETRY, false))
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, times(2)).isClusterRunning(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.FAIL)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should create checkpoint when savepoint mode is automatic and last savepoint is older than savepoint interval`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, false))
        given(context.timeSinceLastSavepointRequestInSeconds()).thenReturn(61)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(context, times(2)).isClusterRunning(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreatingSavepoint)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TriggerSavepoint)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should not create checkpoint when savepoint mode is manual`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        cluster.status.savepointMode = "MANUAL"
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, false))
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        verify(context, times(2)).isClusterRunning(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should suspend cluster when job is finished`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, true))
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, times(1)).isClusterRunning(eq(clusterId))
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
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.SuspendCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `onIdle should stop cluster when there is a manual action`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        Annotations.setManualAction(cluster, ManualAction.STOP)
        val tasks = listOf(ClusterTask.CancelJob, ClusterTask.ClusterHalted)
        given(context.stopCluster(eq(clusterId), any())).thenReturn(OperationResult(OperationStatus.COMPLETED, tasks))
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
    fun `onIdle should scale cluster when desired task managers changed`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        Status.setSavepointPath(context.flinkCluster, "/tmp/000")
        cluster.spec.taskManagers = 4
        val tasks = listOf(ClusterTask.CancelJob, ClusterTask.CreateResources, ClusterTask.ClusterRunning)
        given(context.isClusterRunning(eq(clusterId))).thenReturn(OperationResult(OperationStatus.COMPLETED, false))
        given(context.scaleCluster(eq(clusterId), any())).thenReturn(OperationResult(OperationStatus.COMPLETED, tasks))
        val timestamp = Status.getOperatorTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).resources
        val clusterScaling = ClusterScaling(taskManagers = 4, taskSlots = 1)
        verify(context, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(context, times(2)).isClusterRunning(eq(clusterId))
        verify(context, times(1)).scaleCluster(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.action).isEqualTo(TaskAction.NEXT)
        assertThat(result.output).isEqualTo("[name=test] Rescaling cluster...")
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
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