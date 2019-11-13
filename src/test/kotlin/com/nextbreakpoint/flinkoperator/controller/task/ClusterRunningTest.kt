package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.controller.OperatorState
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
    private val context = mock(OperatorContext::class.java)
    private val controller = mock(OperatorController::class.java)
    private val resources = mock(OperatorResources::class.java)
    private val time = System.currentTimeMillis()
    private val task = ClusterRunning()

    @BeforeEach
    fun configure() {
        given(context.actionTimestamp).thenReturn(0L)
        given(context.operatorTimestamp).thenReturn(time)
        given(context.controller).thenReturn(controller)
        given(context.resources).thenReturn(resources)
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        val actualBootstrapDigest = CustomResources.computeDigest(cluster.spec?.bootstrap)
        val actualRuntimeDigest = CustomResources.computeDigest(cluster.spec?.runtime)
        val actualJobManagerDigest = CustomResources.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = CustomResources.computeDigest(cluster.spec?.taskManager)
        OperatorState.setBootstrapDigest(cluster, actualBootstrapDigest)
        OperatorState.setRuntimeDigest(cluster, actualRuntimeDigest)
        OperatorState.setJobManagerDigest(cluster, actualJobManagerDigest)
        OperatorState.setTaskManagerDigest(cluster, actualTaskManagerDigest)
        OperatorState.appendTasks(cluster, listOf(OperatorTask.ClusterRunning))
        OperatorState.setTaskManagers(cluster, 1)
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
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getNextOperatorTask(cluster)).isNull()
        assertThat(OperatorState.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
        assertThat(OperatorState.getTaskAttempts(cluster)).isEqualTo(0)
    }

    @Test
    fun `onAwaiting should return expected result`() {
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onIdle should fail when job manager digest is missing`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        OperatorState.setBootstrapDigest(cluster, "123")
        OperatorState.setRuntimeDigest(cluster, "123")
        OperatorState.setTaskManagerDigest(cluster, "123")
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onIdle should fail when task manager digest is missing`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        OperatorState.setBootstrapDigest(cluster, "123")
        OperatorState.setRuntimeDigest(cluster, "123")
        OperatorState.setJobManagerDigest(cluster, "123")
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onIdle should fail when flink job digest is missing`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        OperatorState.setRuntimeDigest(cluster, "123")
        OperatorState.setJobManagerDigest(cluster, "123")
        OperatorState.setTaskManagerDigest(cluster, "123")
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onIdle should fail when flink image digest is missing`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        OperatorState.setBootstrapDigest(cluster, "123")
        OperatorState.setJobManagerDigest(cluster, "123")
        OperatorState.setTaskManagerDigest(cluster, "123")
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onIdle should do nothing when digests didn't changed`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink image digest changed but status prevents restart`() {
        OperatorState.setRuntimeDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.Terminated)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).resources
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink job digest changed but status prevents restart`() {
        OperatorState.setBootstrapDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.Terminated)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).resources
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should restart cluster when flink image digest changed`() {
        OperatorState.setRuntimeDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StoppingCluster)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CancelJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TerminatePods)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DeleteResources)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StartingCluster)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CreateResources)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DeleteBootstrapJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CreateBootstrapJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StartJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart cluster when job manager digest changed`() {
        OperatorState.setJobManagerDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StoppingCluster)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CancelJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TerminatePods)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DeleteResources)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StartingCluster)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CreateResources)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DeleteBootstrapJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CreateBootstrapJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StartJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart cluster when task manager digest changed`() {
        OperatorState.setTaskManagerDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StoppingCluster)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CancelJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TerminatePods)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DeleteResources)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StartingCluster)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CreateResources)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DeleteBootstrapJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CreateBootstrapJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StartJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when job digest changed`() {
        OperatorState.setBootstrapDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.UpdatingCluster)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CancelJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DeleteBootstrapJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CreateBootstrapJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StartJob)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.ClusterRunning)
    }

    @Test
    fun `onIdle should do nothing for at least 10 seconds`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorState.updateSavepointTimestamp(cluster)
        OperatorState.setTaskAttempts(cluster, 3)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.currentTimeMillis()).thenReturn(time + 10000)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should update attempts if cluster is running`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorState.updateSavepointTimestamp(cluster)
        OperatorState.setTaskAttempts(cluster, 2)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, false))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getTaskAttempts(cluster)).isEqualTo(0)
    }

    @Test
    fun `onIdle should increment attempts after 10 seconds when cluster is not running`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorState.updateSavepointTimestamp(cluster)
        OperatorState.setTaskAttempts(cluster, 2)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getTaskAttempts(cluster)).isEqualTo(3)
    }

    @Test
    fun `onIdle should change status when cluster is not running after 3 attempts`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorState.updateSavepointTimestamp(cluster)
        OperatorState.setTaskAttempts(cluster, 3)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should create checkpoint when savepoint mode is automatic and last savepoint is older than savepoint interval`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorState.updateSavepointTimestamp(cluster)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, false))
        given(controller.currentTimeMillis()).thenReturn(timestamp + 60000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CreatingSavepoint)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TriggerSavepoint)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.ClusterRunning)
    }

    @Test
    fun `onIdle should not create checkpoint when savepoint mode is manual and last savepoint is older than savepoint interval`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorState.updateSavepointTimestamp(cluster)
        cluster.spec.operator.savepointMode = "MANUAL"
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, false))
        given(controller.currentTimeMillis()).thenReturn(timestamp + 60000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).resources
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should suspend cluster when job is finished`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorState.updateSavepointTimestamp(cluster)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, true))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.StoppingCluster)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TerminatePods)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.SuspendCluster)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.ClusterHalted)
    }

    @Test
    fun `onIdle should stop cluster when there is a manual action`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorAnnotations.setManualAction(cluster, ManualAction.STOP)
        val tasks = listOf(OperatorTask.CancelJob, OperatorTask.ClusterHalted)
        given(controller.stopCluster(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, tasks))
        val actionTimestamp = OperatorAnnotations.getActionTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        val options = StopOptions(withoutSavepoint = false, deleteResources = false)
        verify(controller, times(1)).stopCluster(eq(clusterId), eq(options))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isEqualTo("")
        assertThat(actionTimestamp).isNotEqualTo(OperatorAnnotations.getActionTimestamp(cluster))
        assertThat(OperatorAnnotations.getManualAction(cluster)).isEqualTo(ManualAction.NONE)
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