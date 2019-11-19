package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.Status
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
    private val controller = mock(OperationController::class.java)
    private val resources = mock(CachedResources::class.java)
    private val clusterScaling = ClusterScaling(taskManagers = 1, taskSlots = 1)
    private val time = System.currentTimeMillis()
    private val task = ClusterHalted()

    @BeforeEach
    fun configure() {
        given(context.actionTimestamp).thenReturn(time)
        given(context.operatorTimestamp).thenReturn(time)
        given(context.controller).thenReturn(controller)
        given(context.resources).thenReturn(resources)
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
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
    }

    @Test
    fun `onExecuting should return expected result`() {
        val timestamp = Status.getOperatorTimestamp(cluster)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getTaskAttempts(cluster)).isEqualTo(0)
    }

    @Test
    fun `onAwaiting should return expected result`() {
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onIdle should fail when job manager digest is missing`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        Status.setBootstrapDigest(cluster, "123")
        Status.setRuntimeDigest(cluster, "123")
        Status.setTaskManagerDigest(cluster, "123")
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
        Status.setBootstrapDigest(cluster, "123")
        Status.setRuntimeDigest(cluster, "123")
        Status.setJobManagerDigest(cluster, "123")
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
        Status.setRuntimeDigest(cluster, "123")
        Status.setJobManagerDigest(cluster, "123")
        Status.setTaskManagerDigest(cluster, "123")
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
        Status.setBootstrapDigest(cluster, "123")
        Status.setJobManagerDigest(cluster, "123")
        Status.setTaskManagerDigest(cluster, "123")
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
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when cluster status is failed and digests didn't changed`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should do nothing when flink image digest changed but status prevents restart`() {
        Status.setRuntimeDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).operatorTimestamp
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
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
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).operatorTimestamp
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and flink image digest changed`() {
        System.setProperty("disableReplaceStrategy", "true")
        Status.setRuntimeDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and flink image digest changed and replace strategy is enabled`() {
        System.setProperty("disableReplaceStrategy", "false")
        Status.setRuntimeDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ReplaceResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and job manager digest changed`() {
        System.setProperty("disableReplaceStrategy", "true")
        Status.setJobManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and job manager digest changed and replace strategy is enabled`() {
        System.setProperty("disableReplaceStrategy", "false")
        Status.setJobManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ReplaceResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and task manager digest changed`() {
        System.setProperty("disableReplaceStrategy", "true")
        Status.setTaskManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and task manager digest changed and replace strategy is enabled`() {
        System.setProperty("disableReplaceStrategy", "false")
        Status.setTaskManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ReplaceResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart cluster when cluster status is suspended and flink job digest changed`() {
        Status.setBootstrapDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and flink image digest changed`() {
        System.setProperty("disableReplaceStrategy", "true")
        Status.setRuntimeDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and flink image digest changed and replace strategy is enabled`() {
        System.setProperty("disableReplaceStrategy", "false")
        Status.setRuntimeDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ReplaceResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and job manager digest changed`() {
        System.setProperty("disableReplaceStrategy", "true")
        Status.setJobManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and job manager digest changed and replace strategy is enabled`() {
        System.setProperty("disableReplaceStrategy", "false")
        Status.setJobManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ReplaceResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and task manager digest changed`() {
        System.setProperty("disableReplaceStrategy", "true")
        Status.setTaskManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StoppingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.TerminatePods)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should restart job when cluster status is failed and task manager digest changed and replace strategy is enabled`() {
        System.setProperty("disableReplaceStrategy", "false")
        Status.setTaskManagerDigest(cluster, "123")
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(context.flinkCluster).thenReturn(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.UpdatingCluster)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ReplaceResources)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getCurrentTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should do nothing for at least 10 seconds when cluster status is failed`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        given(controller.currentTimeMillis()).thenReturn(time + 10000)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should check if cluster is running after 10 seconds when cluster status is failed`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
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
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should change status after 10 seconds when cluster status is failed but cluster is running`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
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
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getNextOperatorTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should not restart job when cluster status is failed and cluster is not ready`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        Status.setTaskAttempts(cluster, 3)
        val timestamp = Status.getOperatorTimestamp(cluster)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.AWAIT, null))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(Status.getTaskAttempts(cluster)).isEqualTo(0)
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
    }

    @Test
    fun `onIdle should increment attempts after 10 seconds when cluster status is failed but cluster is ready`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        Status.setTaskAttempts(cluster, 2)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getTaskAttempts(cluster)).isEqualTo(3)
        assertThat(Status.getNextOperatorTask(cluster)).isNull()
    }

    @Test
    fun `onIdle should restart job when cluster status is failed but cluster is ready after 3 attempts`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val timestamp = Status.getOperatorTimestamp(cluster)
        Status.setTaskAttempts(cluster, 3)
        cluster.spec.operator.jobRestartPolicy = "ALWAYS"
        given(controller.isClusterRunning(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, false))
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(controller.currentTimeMillis()).thenReturn(time + 10000 + 1)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).operatorTimestamp
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verifyNoMoreInteractions(context)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).isClusterRunning(eq(clusterId))
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
        assertThat(timestamp).isNotEqualTo(Status.getOperatorTimestamp(cluster))
        assertThat(Status.getNextOperatorTask(cluster)).isEqualTo(ClusterTask.DeleteBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getNextOperatorTask(cluster)).isEqualTo(ClusterTask.CreateBootstrapJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getNextOperatorTask(cluster)).isEqualTo(ClusterTask.StartJob)
        Status.selectNextTask(cluster)
        assertThat(Status.getNextOperatorTask(cluster)).isEqualTo(ClusterTask.ClusterRunning)
    }

    @Test
    fun `onIdle should stop cluster when there is a manual action`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        Annotations.setManualAction(cluster, ManualAction.START)
        val tasks = listOf(ClusterTask.StartJob, ClusterTask.ClusterRunning)
        given(controller.startCluster(eq(clusterId), KotlinMockito.any())).thenReturn(Result(ResultStatus.SUCCESS, tasks))
        val actionTimestamp = Annotations.getActionTimestamp(cluster)
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).controller
        verify(context, atLeastOnce()).clusterId
        verifyNoMoreInteractions(context)
        val options = StartOptions(withoutSavepoint = false)
        verify(controller, times(1)).startCluster(eq(clusterId), eq(options))
        verifyNoMoreInteractions(controller)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isEqualTo("[name=test] ")
        assertThat(actionTimestamp).isNotEqualTo(Annotations.getActionTimestamp(cluster))
        assertThat(Annotations.getManualAction(cluster)).isEqualTo(ManualAction.NONE)
    }

    @Test
    fun `onFailed should return expected result`() {
        val result = task.onFailed(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }
}