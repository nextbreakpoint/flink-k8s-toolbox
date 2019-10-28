package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
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

class ClusterRunningTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster("test", "flink")
    private val context = mock(OperatorContext::class.java)
    private val controller = mock(OperatorController::class.java)
    private val resources = mock(OperatorResources::class.java)
    private val time = System.currentTimeMillis()
    private val task = ClusterRunning()

    @BeforeEach
    fun configure() {
        given(context.lastUpdated).thenReturn(time)
        given(context.controller).thenReturn(controller)
        given(context.resources).thenReturn(resources)
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        val actualFlinkJobDigest = CustomResources.computeDigest(cluster.spec?.flinkJob)
        val actualFlinkImageDigest = CustomResources.computeDigest(cluster.spec?.flinkImage)
        val actualJobManagerDigest = CustomResources.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = CustomResources.computeDigest(cluster.spec?.taskManager)
        OperatorAnnotations.setFlinkJobDigest(cluster, actualFlinkJobDigest)
        OperatorAnnotations.setFlinkImageDigest(cluster, actualFlinkImageDigest)
        OperatorAnnotations.setJobManagerDigest(cluster, actualJobManagerDigest)
        OperatorAnnotations.setTaskManagerDigest(cluster, actualTaskManagerDigest)
        OperatorAnnotations.appendTasks(cluster, listOf(OperatorTask.CLUSTER_RUNNING))
    }

    @Test
    fun `onExecuting should return expected result`() {
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getNextOperatorTask(cluster)).isNull()
        assertThat(OperatorAnnotations.getClusterStatus(cluster)).isEqualTo(ClusterStatus.RUNNING)
        assertThat(OperatorAnnotations.getOperatorTaskAttempts(cluster)).isEqualTo(0)
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
        val cluster = TestFactory.aCluster("test", "flink")
        OperatorAnnotations.setFlinkJobDigest(cluster, "123")
        OperatorAnnotations.setFlinkImageDigest(cluster, "123")
        OperatorAnnotations.setTaskManagerDigest(cluster, "123")
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
        val cluster = TestFactory.aCluster("test", "flink")
        OperatorAnnotations.setFlinkJobDigest(cluster, "123")
        OperatorAnnotations.setFlinkImageDigest(cluster, "123")
        OperatorAnnotations.setJobManagerDigest(cluster, "123")
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
        val cluster = TestFactory.aCluster("test", "flink")
        OperatorAnnotations.setFlinkImageDigest(cluster, "123")
        OperatorAnnotations.setJobManagerDigest(cluster, "123")
        OperatorAnnotations.setTaskManagerDigest(cluster, "123")
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
        val cluster = TestFactory.aCluster("test", "flink")
        OperatorAnnotations.setFlinkJobDigest(cluster, "123")
        OperatorAnnotations.setJobManagerDigest(cluster, "123")
        OperatorAnnotations.setTaskManagerDigest(cluster, "123")
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
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink image digest changed but status prevents restart`() {
        OperatorAnnotations.setFlinkImageDigest(cluster, "123")
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.TERMINATED)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).lastUpdated
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink job digest changed but status prevents restart`() {
        OperatorAnnotations.setFlinkJobDigest(cluster, "123")
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.TERMINATED)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).lastUpdated
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should restart cluster when flink image digest changed`() {
        OperatorAnnotations.setFlinkImageDigest(cluster, "123")
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CANCEL_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.TERMINATE_PODS)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_RESOURCES)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CREATE_RESOURCES)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should restart cluster when job manager digest changed`() {
        OperatorAnnotations.setJobManagerDigest(cluster, "123")
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CANCEL_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.TERMINATE_PODS)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_RESOURCES)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CREATE_RESOURCES)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should restart cluster when task manager digest changed`() {
        OperatorAnnotations.setTaskManagerDigest(cluster, "123")
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CANCEL_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.TERMINATE_PODS)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_RESOURCES)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CREATE_RESOURCES)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should restart job when job digest changed`() {
        OperatorAnnotations.setFlinkJobDigest(cluster, "123")
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CANCEL_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should do nothing for at least 10 seconds`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        OperatorAnnotations.updateSavepointTimestamp(cluster)
        OperatorAnnotations.setOperatorTaskAttempts(cluster, 3)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.currentTimeMillis()).thenReturn(time + 10000)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should update attempts if cluster is running`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        OperatorAnnotations.updateSavepointTimestamp(cluster)
        OperatorAnnotations.setOperatorTaskAttempts(cluster, 2)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, false))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
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
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getOperatorTaskAttempts(cluster)).isEqualTo(0)
    }

    @Test
    fun `onIdle should increment attempts after 10 seconds when cluster is not running`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        OperatorAnnotations.updateSavepointTimestamp(cluster)
        OperatorAnnotations.setOperatorTaskAttempts(cluster, 2)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
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
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        assertThat(OperatorAnnotations.getOperatorTaskAttempts(cluster)).isEqualTo(3)
    }

    @Test
    fun `onIdle should change status when cluster is not running after 3 attempts`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        OperatorAnnotations.updateSavepointTimestamp(cluster)
        OperatorAnnotations.setOperatorTaskAttempts(cluster, 3)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
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
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should create checkpoint when savepoint mode is automatic and last savepoint is older than savepoint interval`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        OperatorAnnotations.updateSavepointTimestamp(cluster)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, false))
        given(controller.currentTimeMillis()).thenReturn(timestamp + 60000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
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
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CHECKPOINTING_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CREATE_SAVEPOINT)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should not create checkpoint when savepoint mode is manual and last savepoint is older than savepoint interval`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        OperatorAnnotations.updateSavepointTimestamp(cluster)
        cluster.spec.flinkOperator.savepointMode = "MANUAL"
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, false))
        given(controller.currentTimeMillis()).thenReturn(timestamp + 60000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
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
        assertThat(timestamp).isEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should suspend cluster when job is finished`() {
        OperatorAnnotations.setClusterStatus(cluster, ClusterStatus.RUNNING)
        OperatorAnnotations.updateSavepointTimestamp(cluster)
        val timestamp = OperatorAnnotations.getOperatorTimestamp(cluster)
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, true))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
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
        assertThat(timestamp).isNotEqualTo(OperatorAnnotations.getOperatorTimestamp(cluster))
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.TERMINATE_PODS)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.SUSPEND_CLUSTER)
        OperatorAnnotations.selectNextTask(cluster)
        assertThat(OperatorAnnotations.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_HALTED)
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