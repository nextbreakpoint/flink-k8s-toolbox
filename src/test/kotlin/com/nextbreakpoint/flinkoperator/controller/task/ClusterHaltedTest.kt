package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorState
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

class ClusterHaltedTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster("test", "flink")
    private val context = mock(OperatorContext::class.java)
    private val controller = mock(OperatorController::class.java)
    private val resources = mock(OperatorResources::class.java)
    private val time = System.currentTimeMillis()
    private val task = ClusterHalted()

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
        OperatorState.setFlinkJobDigest(cluster, actualFlinkJobDigest)
        OperatorState.setFlinkImageDigest(cluster, actualFlinkImageDigest)
        OperatorState.setJobManagerDigest(cluster, actualJobManagerDigest)
        OperatorState.setTaskManagerDigest(cluster, actualTaskManagerDigest)
        OperatorState.appendTasks(cluster, listOf(OperatorTask.CLUSTER_HALTED))
        OperatorState.setOperatorTaskAttempts(cluster, 1)
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
        assertThat(OperatorState.getOperatorTaskAttempts(cluster)).isEqualTo(0)
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
        OperatorState.setFlinkJobDigest(cluster, "123")
        OperatorState.setFlinkImageDigest(cluster, "123")
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
        val cluster = TestFactory.aCluster("test", "flink")
        OperatorState.setFlinkJobDigest(cluster, "123")
        OperatorState.setFlinkImageDigest(cluster, "123")
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
        val cluster = TestFactory.aCluster("test", "flink")
        OperatorState.setFlinkImageDigest(cluster, "123")
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
        val cluster = TestFactory.aCluster("test", "flink")
        OperatorState.setFlinkJobDigest(cluster, "123")
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
    fun `onIdle should do nothing when cluster status is suspended and digests didn't changed`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.SUSPENDED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
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
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when cluster status is failed and digests didn't changed`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
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
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink image digest changed but status prevents restart`() {
        OperatorState.setFlinkImageDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
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
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink job digest changed but status prevents restart`() {
        OperatorState.setFlinkJobDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.RUNNING)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
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
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and flink image digest changed`() {
        OperatorState.setFlinkImageDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.SUSPENDED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TERMINATE_PODS)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CREATE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and job manager digest changed`() {
        OperatorState.setJobManagerDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.SUSPENDED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TERMINATE_PODS)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CREATE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and task manager digest changed`() {
        OperatorState.setTaskManagerDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.SUSPENDED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TERMINATE_PODS)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CREATE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and flink job digest changed`() {
        OperatorState.setFlinkJobDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.SUSPENDED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and flink image digest changed`() {
        OperatorState.setFlinkImageDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TERMINATE_PODS)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CREATE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and job manager digest changed`() {
        OperatorState.setJobManagerDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TERMINATE_PODS)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CREATE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and task manager digest changed`() {
        OperatorState.setTaskManagerDigest(cluster, "123")
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STOPPING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.TERMINATE_PODS)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CREATE_RESOURCES)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getCurrentTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should do nothing for at least 10 seconds when cluster status is failed`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
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
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should check if cluster is running after 10 seconds when cluster status is failed`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
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
        assertThat(timestamp).isEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should change status after 10 seconds when cluster status is failed but cluster is running`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
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
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getNextOperatorTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
    }

    @Test
    fun `onIdle should not restart job when cluster status is failed and cluster is not ready`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        OperatorState.setOperatorTaskAttempts(cluster, 3)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        cluster.spec.flinkOperator.jobRestartPolicy = "ONFAILURE"
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verify(controller, times(1)).isClusterReady(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(OperatorState.getOperatorTaskAttempts(cluster)).isEqualTo(0)
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should increment attempts after 10 seconds when cluster status is failed but cluster is ready`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        OperatorState.setOperatorTaskAttempts(cluster, 2)
        cluster.spec.flinkOperator.jobRestartPolicy = "ONFAILURE"
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verify(controller, times(1)).isClusterReady(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getOperatorTaskAttempts(cluster)).isEqualTo(3)
        assertThat(OperatorState.getNextOperatorTask(cluster)).isNull()
    }

    @Test
    fun `onIdle should restart job when cluster status is failed but cluster is ready after 3 attempts`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.FAILED)
        val timestamp = OperatorState.getOperatorTimestamp(cluster)
        OperatorState.setOperatorTaskAttempts(cluster, 3)
        cluster.spec.flinkOperator.jobRestartPolicy = "ONFAILURE"
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.isClusterReady(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).lastUpdated
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verify(controller, times(1)).isClusterReady(eq(clusterId))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(OperatorState.getOperatorTimestamp(cluster))
        assertThat(OperatorState.getNextOperatorTask(cluster)).isEqualTo(OperatorTask.DELETE_UPLOAD_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getNextOperatorTask(cluster)).isEqualTo(OperatorTask.UPLOAD_JAR)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getNextOperatorTask(cluster)).isEqualTo(OperatorTask.START_JOB)
        OperatorState.selectNextTask(cluster)
        assertThat(OperatorState.getNextOperatorTask(cluster)).isEqualTo(OperatorTask.CLUSTER_RUNNING)
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