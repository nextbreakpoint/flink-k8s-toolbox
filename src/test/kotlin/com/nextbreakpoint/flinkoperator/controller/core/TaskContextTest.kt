package com.nextbreakpoint.flinkoperator.mediator.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskMediator
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.apache.log4j.Logger
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class TaskContextTest {
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val clusterSelector = ClusterSelector(name = "test", namespace = "flink", uuid = "123")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val savepointOptions = SavepointOptions(targetPath = "file:///tmp")
    private val clusterScaling = ClusterScaling(taskManagers = 1, taskSlots = 1)
    private val logger = mock(Logger::class.java)
    private val mediator = mock(TaskMediator::class.java)
    private val context = TaskContext(logger, mediator)

    @BeforeEach
    fun configure() {
        given(mediator.clusterSelector).thenReturn(clusterSelector)
        given(mediator.hasFinalizer()).thenReturn(true)
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.isJobFinished(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        given(mediator.isJobRunning(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.isJobFailed(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        given(mediator.arePodsRunning(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.getJobManagerReplicas()).thenReturn(1)
        given(mediator.getTaskManagerReplicas()).thenReturn(1)
        given(mediator.getClusterScale()).thenReturn(clusterScaling)
        given(mediator.doesBootstrapJobExists()).thenReturn(true)
        given(mediator.doesJobManagerServiceExists()).thenReturn(true)
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(true)
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(true)
        given(mediator.timeSinceLastUpdateInSeconds()).thenReturn(2L)
        given(mediator.timeSinceLastSavepointRequestInSeconds()).thenReturn(30L)
        given(mediator.createBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.getSavepointRequest()).thenReturn(null)
        given(mediator.getSavepointOtions()).thenReturn(savepointOptions)
    }

    @Test
    fun `should remove finalizer`() {
        context.removeFinalizer()
        verify(mediator, times(1)).removeFinalizer()
    }

    @Test
    fun `should change status on task timed out event`() {
        context.onTaskTimeOut()
        verify(mediator, times(1)).setClusterStatus(any())
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Failed))
    }

    @Test
    fun `should change status on cluster terminated event`() {
        context.onClusterTerminated()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Terminated))
    }

    @Test
    fun `should change status on cluster suspended event`() {
        context.onClusterSuspended()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Suspended))
    }

    @Test
    fun `should change status on cluster started event`() {
        context.onClusterStarted()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Running))
    }

    @Test
    fun `should change status on resource diverged event`() {
        context.onResourceDiverged()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Restarting))
    }

    @Test
    fun `should change status on job cancelled event`() {
        context.onClusterReadyToStop()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Stopping))
    }

    @Test
    fun `should change status on job finished event`() {
        context.onJobFinished()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Finished))
    }

    @Test
    fun `should change status on job failed event`() {
        context.onJobFailed()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Failed))
    }

    @Test
    fun `should change status on resource initialized event`() {
        context.onResourceInitialise()
        verify(mediator, times(1)).initializeAnnotations()
        verify(mediator, times(1)).initializeStatus()
        verify(mediator, times(1)).updateDigests()
        verify(mediator, times(1)).addFinalizer()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Starting))
    }

    @Test
    fun `should change status on resource changed event`() {
        context.onResourceChanged()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Restarting))
    }

    @Test
    fun `should change status on resource deleted event`() {
        context.onResourceDeleted()
        verify(mediator, times(1)).setDeleteResources(true)
        verify(mediator, times(1)).resetManualAction()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Stopping))
    }

    @Test
    fun `should change status on resource ready to update event`() {
        context.onClusterReadyToUpdate()
        verify(mediator, times(1)).updateDigests()
        verify(mediator, times(1)).updateStatus()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Starting))
    }

    @Test
    fun `should change status on resource ready to restart event`() {
        context.onClusterReadyToRestart()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Updating))
    }

    @Test
    fun `should change status on cluster ready to scale up event`() {
        given(mediator.getTaskManagers()).thenReturn(2)
        context.onClusterReadyToScale()
        verify(mediator, times(1)).rescaleCluster()
        verify(mediator, times(1)).getTaskManagers()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Starting))
    }

    @Test
    fun `should change status on cluster ready to scale down event`() {
        given(mediator.getTaskManagers()).thenReturn(0)
        context.onClusterReadyToScale()
        verify(mediator, times(1)).rescaleCluster()
        verify(mediator, times(1)).getTaskManagers()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Stopping))
    }

    @Test
    fun `should change status on resource scaled event`() {
        context.onResourceScaled()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Scaling))
    }

    @Test
    fun `cancelJob should return true when jobmanager service is not present`() {
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        val result = context.cancelJob()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when jobmanager pod is not present`() {
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(false)
        val result = context.cancelJob()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when taskmanager pod is not present`() {
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(false)
        val result = context.cancelJob()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when pods are not running`() {
        given(mediator.arePodsRunning(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = context.cancelJob()
        verify(mediator, times(1)).arePodsRunning(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when there is an error checking pods running and job hasn't been stopped`() {
        given(mediator.arePodsRunning(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        given(mediator.stopJob(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = context.cancelJob()
        verify(mediator, times(1)).arePodsRunning(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is not required and job has been stopped`() {
        given(mediator.isSavepointRequired()).thenReturn(false)
        given(mediator.stopJob(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        val result = context.cancelJob()
        verify(mediator, times(1)).isSavepointRequired()
        verify(mediator, times(1)).stopJob(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return false when savepoint is not required and job hasn't been stopped`() {
        given(mediator.isSavepointRequired()).thenReturn(false)
        given(mediator.stopJob(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = context.cancelJob()
        verify(mediator, times(1)).isSavepointRequired()
        verify(mediator, times(1)).stopJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is present but savepoint is not completed`() {
        given(mediator.isSavepointRequired()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(savepointRequest)
        given(mediator.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        val result = context.cancelJob()
        verify(mediator, times(1)).isSavepointRequired()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).querySavepoint(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is present but savepoint can't be completed`() {
        given(mediator.isSavepointRequired()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(savepointRequest)
        given(mediator.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.ERROR, null))
        val result = context.cancelJob()
        verify(mediator, times(1)).isSavepointRequired()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).querySavepoint(any(), any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is present but savepoint takes too long`() {
        given(mediator.isSavepointRequired()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(savepointRequest)
        given(mediator.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.timeSinceLastUpdateInSeconds()).thenReturn(301)
        val result = context.cancelJob()
        verify(mediator, times(1)).isSavepointRequired()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).querySavepoint(any(), any())
        verify(mediator, times(1)).timeSinceLastUpdateInSeconds()
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is present and savepoint is completed`() {
        given(mediator.isSavepointRequired()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(savepointRequest)
        given(mediator.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, "/tmp/1"))
        val result = context.cancelJob()
        verify(mediator, times(1)).resetSavepointRequest()
        verify(mediator, times(1)).setSavepointPath("/tmp/1")
        verify(mediator, times(1)).querySavepoint(any(), any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is not present and job can't be cancelled`() {
        given(mediator.isSavepointRequired()).thenReturn(true)
        given(mediator.cancelJob(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        val result = context.cancelJob()
        verify(mediator, times(1)).isSavepointRequired()
        verify(mediator, times(1)).cancelJob(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is not present and job has been cancelled`() {
        given(mediator.isSavepointRequired()).thenReturn(true)
        given(mediator.cancelJob(any(), any())).thenReturn(OperationResult(OperationStatus.OK, savepointRequest))
        val result = context.cancelJob()
        verify(mediator, times(1)).setSavepointRequest(savepointRequest)
        verify(mediator, times(1)).isSavepointRequired()
        verify(mediator, times(1)).cancelJob(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is not present and job has been stopped`() {
        given(mediator.isSavepointRequired()).thenReturn(true)
        given(mediator.cancelJob(any(), any())).thenReturn(OperationResult(OperationStatus.OK, SavepointRequest("", "")))
        val result = context.cancelJob()
        verify(mediator, times(1)).isSavepointRequired()
        verify(mediator, times(1)).cancelJob(any(), any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is not present and there is an error cancelling the job`() {
        given(mediator.isSavepointRequired()).thenReturn(true)
        given(mediator.cancelJob(any(), any())).thenReturn(OperationResult(OperationStatus.ERROR, null))
        val result = context.cancelJob()
        verify(mediator, times(1)).isSavepointRequired()
        verify(mediator, times(1)).cancelJob(any(), any())
        assertThat(result).isTrue()
    }

    @Test
    fun `startCluster should return false when jobmanager service is not present`() {
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        val result = context.startCluster()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when jobmanager pod is not present`() {
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(false)
        val result = context.startCluster()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when taskmanager pod is not present`() {
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(false)
        val result = context.startCluster()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when jobmanager replicas is not one`() {
        given(mediator.getJobManagerReplicas()).thenReturn(0)
        val result = context.startCluster()
        verify(mediator, times(1)).getJobManagerReplicas()
        verify(mediator, times(1)).restartPods(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when taskmanager replicas is not the same as current resource taskmanagers`() {
        given(mediator.getTaskManagerReplicas()).thenReturn(0)
        val result = context.startCluster()
        verify(mediator, times(1)).getClusterScale()
        verify(mediator, times(1)).getTaskManagerReplicas()
        verify(mediator, times(1)).restartPods(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap in not defined and there is an error checking cluster is ready`() {
        given(mediator.isBootstrapPresent()).thenReturn(false)
        given(mediator.isClusterReady(any(), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).isClusterReady(any(), eq(clusterScaling))
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap in not defined and cluster is not ready`() {
        given(mediator.isBootstrapPresent()).thenReturn(false)
        given(mediator.isClusterReady(any(), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).isClusterReady(any(), eq(clusterScaling))
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return true when bootstrap in not defined and cluster is ready`() {
        given(mediator.isBootstrapPresent()).thenReturn(false)
        given(mediator.isClusterReady(any(), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.OK, true))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).isClusterReady(any(), eq(clusterScaling))
        assertThat(result).isTrue()
    }

    @Test
    fun `startCluster should return false when bootstrap job is present and there is an error when listing jobs`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.isJobRunning(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).isJobRunning(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is present but job is not running`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.isJobRunning(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).isJobRunning(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return true when bootstrap job is present and job is running`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.isJobRunning(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).isJobRunning(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and there is an error checking readiness`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.isClusterReady(any(), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).isClusterReady(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is not ready`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.isClusterReady(any(), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).isClusterReady(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is ready but jar hasn't been removed`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.isClusterReady(any(), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.removeJar(any())).thenReturn(OperationResult(OperationStatus.ERROR, null))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).isClusterReady(any(), any())
        verify(mediator, times(1)).removeJar(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is ready and jar has been removed but there is an error stopping job`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.isClusterReady(any(), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.removeJar(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.stopJob(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).isClusterReady(any(), any())
        verify(mediator, times(1)).removeJar(any())
        verify(mediator, times(1)).stopJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is ready and jar has been removed but job hasn't been stopped`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.isClusterReady(any(), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.removeJar(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.stopJob(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).isClusterReady(any(), any())
        verify(mediator, times(1)).removeJar(any())
        verify(mediator, times(1)).stopJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is ready and jar has been removed and job has been stopped but bootstrap job hasn't been created`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.isClusterReady(any(), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.removeJar(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.stopJob(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.createBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.ERROR, "test"))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).isClusterReady(any(), any())
        verify(mediator, times(1)).removeJar(any())
        verify(mediator, times(1)).stopJob(any())
        verify(mediator, times(1)).createBootstrapJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is ready and jar has been removed and job has been stopped and bootstrap job has ben created`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.isClusterReady(any(), eq(clusterScaling))).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.removeJar(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.stopJob(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.createBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.OK, "test"))
        val result = context.startCluster()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).isClusterReady(any(), any())
        verify(mediator, times(1)).removeJar(any())
        verify(mediator, times(1)).stopJob(any())
        verify(mediator, times(1)).createBootstrapJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `suspendCluster should return false when pods haven't been stopped`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = context.suspendCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).terminatePods(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `suspendCluster should return false when there is an error checking pods are running`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = context.suspendCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).terminatePods(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `suspendCluster should return false when pods have been stopped but jobmanager service is present`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.deleteJobManagerService(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.doesJobManagerServiceExists()).thenReturn(true)
        val result = context.suspendCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).deleteJobManagerService(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `suspendCluster should return true when pods have been stopped and jobmanager service is not present and bootstrap job is not present`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        val result = context.suspendCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `terminateCluster should return false when pods haven't been stopped`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = context.terminateCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).terminatePods(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when there is an error checking pods are running`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = context.terminateCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).terminatePods(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when pods have been stopped but bootstrap job is present`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.deleteBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.doesBootstrapJobExists()).thenReturn(true)
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesJobManagerPVCExists()).thenReturn(false)
        given(mediator.doesTaskManagerPVCExists()).thenReturn(false)
        val result = context.terminateCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).doesJobManagerPVCExists()
        verify(mediator, times(1)).doesTaskManagerPVCExists()
        verify(mediator, times(1)).deleteBootstrapJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when pods have been stopped but jobmanager service is present`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.deleteJobManagerService(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.doesJobManagerServiceExists()).thenReturn(true)
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesJobManagerPVCExists()).thenReturn(false)
        given(mediator.doesTaskManagerPVCExists()).thenReturn(false)
        val result = context.terminateCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).doesJobManagerPVCExists()
        verify(mediator, times(1)).doesTaskManagerPVCExists()
        verify(mediator, times(1)).deleteJobManagerService(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when pods have been stopped but jobmanager statefulset is present`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.deleteStatefulSets(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(true)
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesJobManagerPVCExists()).thenReturn(false)
        given(mediator.doesTaskManagerPVCExists()).thenReturn(false)
        val result = context.terminateCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).doesJobManagerPVCExists()
        verify(mediator, times(1)).doesTaskManagerPVCExists()
        verify(mediator, times(1)).deleteStatefulSets(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when pods have been stopped but taskmanager statefulset is present`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.deleteStatefulSets(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(true)
        given(mediator.doesJobManagerPVCExists()).thenReturn(false)
        given(mediator.doesTaskManagerPVCExists()).thenReturn(false)
        val result = context.terminateCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).doesJobManagerPVCExists()
        verify(mediator, times(1)).doesTaskManagerPVCExists()
        verify(mediator, times(1)).deleteStatefulSets(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when pods have been stopped but jobmanager pvc is present`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.deletePersistentVolumeClaims(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesJobManagerPVCExists()).thenReturn(true)
        given(mediator.doesTaskManagerPVCExists()).thenReturn(false)
        val result = context.terminateCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).doesJobManagerPVCExists()
        verify(mediator, times(1)).doesTaskManagerPVCExists()
        verify(mediator, times(1)).deletePersistentVolumeClaims(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when pods have been stopped but taskmanager pvc is present`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.deletePersistentVolumeClaims(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesJobManagerPVCExists()).thenReturn(false)
        given(mediator.doesTaskManagerPVCExists()).thenReturn(true)
        val result = context.terminateCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).doesJobManagerPVCExists()
        verify(mediator, times(1)).doesTaskManagerPVCExists()
        verify(mediator, times(1)).deletePersistentVolumeClaims(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return true when pods have been stopped and jobmanager service is not present and bootstrap job is not present and statefulsets are not present and pvcs are not present`() {
        given(mediator.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(mediator.doesJobManagerPVCExists()).thenReturn(false)
        given(mediator.doesTaskManagerPVCExists()).thenReturn(false)
        val result = context.terminateCluster()
        verify(mediator, times(1)).arePodsTerminated(any())
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).doesJobManagerPVCExists()
        verify(mediator, times(1)).doesTaskManagerPVCExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `resetCluster should return false when bootstrap job is present`() {
        given(mediator.deleteBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.doesBootstrapJobExists()).thenReturn(true)
        val result = context.resetCluster()
        verify(mediator, times(1)).doesBootstrapJobExists()
        verify(mediator, times(1)).deleteBootstrapJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `resetCluster should return true when bootstrap job is not present`() {
        given(mediator.deleteBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(mediator.doesBootstrapJobExists()).thenReturn(false)
        val result = context.resetCluster()
        verify(mediator, times(1)).doesBootstrapJobExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when jobmanager service is not present`() {
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        val result = context.hasResourceDiverged()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when jobmanager statefulset is not present`() {
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(false)
        val result = context.hasResourceDiverged()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when taskmanager statefulset is not present`() {
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(false)
        val result = context.hasResourceDiverged()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when jobmanager replicas is not equals to desired value`() {
        given(mediator.getJobManagerReplicas()).thenReturn(0)
        val result = context.hasResourceDiverged()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).getClusterScale()
        verify(mediator, times(1)).getJobManagerReplicas()
        verify(mediator, times(1)).getTaskManagerReplicas()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when taskmanager replicas is not equals to desired value`() {
        given(mediator.getTaskManagerReplicas()).thenReturn(0)
        val result = context.hasResourceDiverged()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).getClusterScale()
        verify(mediator, times(1)).getJobManagerReplicas()
        verify(mediator, times(1)).getTaskManagerReplicas()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return false when jobmanager and taskmanager replicas are equal to desired value`() {
        val result = context.hasResourceDiverged()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).getClusterScale()
        verify(mediator, times(1)).getJobManagerReplicas()
        verify(mediator, times(1)).getTaskManagerReplicas()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasResourceChanged should return true when resource has changed`() {
        given(mediator.computeChanges()).thenReturn(listOf("TEST"))
        val result = context.hasResourceChanged()
        verify(mediator, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceChanged should return false when resource hasn't changed`() {
        given(mediator.computeChanges()).thenReturn(listOf())
        val result = context.hasResourceChanged()
        verify(mediator, times(1)).computeChanges()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFinished should return true when bootstrap is defined and job has finished`() {
        given(mediator.isJobFinished(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        val result = context.hasJobFinished()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).isJobFinished(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `hasJobFinished should return false when bootstrap is defined and job hasn't finished`() {
        given(mediator.isJobFinished(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = context.hasJobFinished()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).isJobFinished(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFinished should return false when bootstrap is defined and there is an error checking job has finished`() {
        given(mediator.isJobFinished(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = context.hasJobFinished()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).isJobFinished(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFinished should return false when bootstrap is not defined`() {
        given(mediator.isBootstrapPresent()).thenReturn(false)
        val result = context.hasJobFinished()
        verify(mediator, times(1)).isBootstrapPresent()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFailed should return true when bootstrap is defined and job has failed`() {
        given(mediator.isJobFailed(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        val result = context.hasJobFailed()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).isJobFailed(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `hasJobFailed should return false when bootstrap is defined and job hasn't failed`() {
        given(mediator.isJobFailed(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = context.hasJobFailed()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).isJobFailed(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFailed should return false when bootstrap is defined and there is an error checking job has failed`() {
        given(mediator.isJobFailed(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = context.hasJobFailed()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).isJobFailed(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFailed should return false when bootstrap is not defined`() {
        given(mediator.isBootstrapPresent()).thenReturn(false)
        val result = context.hasJobFailed()
        verify(mediator, times(1)).isBootstrapPresent()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasTaskTimedOut should return false when task hasn't timed out`() {
        given(mediator.timeSinceLastUpdateInSeconds()).thenReturn(10L)
        val result = context.hasTaskTimedOut()
        verify(mediator, times(1)).timeSinceLastUpdateInSeconds()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasTaskTimedOut should return true when task has timed out`() {
        given(mediator.timeSinceLastUpdateInSeconds()).thenReturn(301L)
        val result = context.hasTaskTimedOut()
        verify(mediator, times(1)).timeSinceLastUpdateInSeconds()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasScaleChanged should return false when scale hasn't changed`() {
        given(mediator.getDesiredTaskManagers()).thenReturn(2)
        given(mediator.getTaskManagers()).thenReturn(2)
        val result = context.hasScaleChanged()
        verify(mediator, times(1)).getDesiredTaskManagers()
        verify(mediator, times(1)).getTaskManagers()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasScaleChanged should return true when scale has changed`() {
        given(mediator.getDesiredTaskManagers()).thenReturn(2)
        given(mediator.getTaskManagers()).thenReturn(1)
        val result = context.hasScaleChanged()
        verify(mediator, times(1)).getDesiredTaskManagers()
        verify(mediator, times(1)).getTaskManagers()
        assertThat(result).isTrue()
    }

    @Test
    fun `isManualActionPresent should return true when manual action is not none`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.START)
        val result = context.isManualActionPresent()
        verify(mediator, times(1)).getManualAction()
        assertThat(result).isTrue()
    }

    @Test
    fun `isManualActionPresent should return false when manual action is none`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.NONE)
        val result = context.isManualActionPresent()
        verify(mediator, times(1)).getManualAction()
        assertThat(result).isFalse()
    }

    @Test
    fun `isResourceDeleted should return false when resource hasn't been deleted`() {
        given(mediator.hasBeenDeleted()).thenReturn(false)
        val result = context.isResourceDeleted()
        verify(mediator, times(1)).hasBeenDeleted()
        assertThat(result).isFalse()
    }

    @Test
    fun `isResourceDeleted should return true when resource has been deleted`() {
        given(mediator.hasBeenDeleted()).thenReturn(true)
        val result = context.isResourceDeleted()
        verify(mediator, times(1)).hasBeenDeleted()
        assertThat(result).isTrue()
    }

    @Test
    fun `shouldRestartJob should return false when restart policy is not always`() {
        given(mediator.getRestartPolicy()).thenReturn("NEVER")
        val result = context.shouldRestart()
        verify(mediator, times(1)).getRestartPolicy()
        assertThat(result).isFalse()
    }

    @Test
    fun `shouldRestartJob should return true when restart policy is always`() {
        given(mediator.getRestartPolicy()).thenReturn("ALWAYS")
        val result = context.shouldRestart()
        verify(mediator, times(1)).getRestartPolicy()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustTerminateResources should return false when resources don't have to be deleted`() {
        given(mediator.isDeleteResources()).thenReturn(false)
        val result = context.mustTerminateResources()
        verify(mediator, times(1)).isDeleteResources()
        assertThat(result).isFalse()
    }

    @Test
    fun `mustTerminateResources should return true when resources have to be deleted`() {
        given(mediator.isDeleteResources()).thenReturn(true)
        val result = context.mustTerminateResources()
        verify(mediator, times(1)).isDeleteResources()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustRecreateResources should return false when only bootstrap changed`() {
        given(mediator.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        val result = context.mustRecreateResources()
        verify(mediator, times(1)).computeChanges()
        assertThat(result).isFalse()
    }

    @Test
    fun `mustRecreateResources should return true when jobmanager spec changed`() {
        given(mediator.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        val result = context.mustRecreateResources()
        verify(mediator, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustRecreateResources should return true when taskmanager spec changed`() {
        given(mediator.computeChanges()).thenReturn(listOf("TASK_MANAGER"))
        val result = context.mustRecreateResources()
        verify(mediator, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustRecreateResources should return true when runtime spec changed`() {
        given(mediator.computeChanges()).thenReturn(listOf("RUNTIME"))
        val result = context.mustRecreateResources()
        verify(mediator, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `ensureServiceExist should do nothing when jobmanager service exists`() {
        given(mediator.doesJobManagerServiceExists()).thenReturn(true)
        context.ensureServiceExist()
        verify(mediator, times(1)).doesJobManagerServiceExists()
    }

    @Test
    fun `ensureServiceExist should create resource when jobmanager service doesn't exists`() {
        given(mediator.doesJobManagerServiceExists()).thenReturn(false)
        given(mediator.createJobManagerService(any())).thenReturn(OperationResult(OperationStatus.OK, "test"))
        context.ensureServiceExist()
        verify(mediator, times(1)).doesJobManagerServiceExists()
        verify(mediator, times(1)).createJobManagerService(eq(clusterSelector))
    }

    @Test
    fun `ensurePodsExists should do nothing when jobmanager statefulset exists`() {
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(true)
        context.ensurePodsExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
    }

    @Test
    fun `ensurePodsExists should do nothing when jobmanager statefulset doesn't exists`() {
        given(mediator.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(mediator.createJobManagerStatefulSet(any())).thenReturn(OperationResult(OperationStatus.OK, "test"))
        context.ensurePodsExists()
        verify(mediator, times(1)).doesJobManagerStatefulSetExists()
        verify(mediator, times(1)).createJobManagerStatefulSet(eq(clusterSelector))
    }

    @Test
    fun `ensurePodsExists should do nothing when taskmanager statefulset exists`() {
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(true)
        context.ensurePodsExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
    }

    @Test
    fun `ensurePodsExists should do nothing when taskmanager statefulset doesn't exists`() {
        given(mediator.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(mediator.createTaskManagerStatefulSet(any())).thenReturn(OperationResult(OperationStatus.OK, "test"))
        context.ensurePodsExists()
        verify(mediator, times(1)).doesTaskManagerStatefulSetExists()
        verify(mediator, times(1)).createTaskManagerStatefulSet(eq(clusterSelector))
    }

    @Test
    fun `executeManualAction should do nothing when manual action is none`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.NONE)
        context.executeManualAction(setOf(ManualAction.NONE))
        verify(mediator, times(1)).getManualAction()
        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `executeManualAction should do nothing when manual action is start but action is not allowed`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.START)
        context.executeManualAction(setOf())
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is stop but action is not allowed`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.STOP)
        context.executeManualAction(setOf())
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is forget savepoint but action is not allowed`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.FORGET_SAVEPOINT)
        context.executeManualAction(setOf())
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is trigger savepoint but action is not allowed`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        context.executeManualAction(setOf())
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is trigger savepoint and bootstrap is not defined`() {
        given(mediator.isBootstrapPresent()).thenReturn(false)
        given(mediator.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        context.executeManualAction(setOf(ManualAction.TRIGGER_SAVEPOINT))
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is start`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.START)
        context.executeManualAction(setOf(ManualAction.START))
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Starting))
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is stop and job doesn't need to be cancelled`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.STOP)
        context.executeManualAction(setOf(ManualAction.STOP), false)
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).setDeleteResources(eq(true))
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Stopping))
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is stop and job needs to be cancelled`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.STOP)
        context.executeManualAction(setOf(ManualAction.STOP), true)
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).setClusterStatus(eq(ClusterStatus.Cancelling))
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is forget savepoint`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.FORGET_SAVEPOINT)
        context.executeManualAction(setOf(ManualAction.FORGET_SAVEPOINT))
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).setSavepointPath(eq(""))
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is trigger savepoint and savepoint request is not present`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        given(mediator.getSavepointRequest()).thenReturn(null)
        given(mediator.triggerSavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, savepointRequest))
        context.executeManualAction(setOf(ManualAction.TRIGGER_SAVEPOINT))
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
        verify(mediator, times(1)).setSavepointRequest(eq(savepointRequest))
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should not change status when manual action is trigger savepoint and savepoint request is present`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        given(mediator.getSavepointRequest()).thenReturn(savepointRequest)
        context.executeManualAction(setOf(ManualAction.TRIGGER_SAVEPOINT))
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should not change status when manual action is trigger savepoint and savepoint request can't be created`() {
        given(mediator.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        given(mediator.getSavepointRequest()).thenReturn(null)
        given(mediator.triggerSavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.ERROR, savepointRequest))
        context.executeManualAction(setOf(ManualAction.TRIGGER_SAVEPOINT))
        verify(mediator, times(1)).getManualAction()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).resetManualAction()
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is not present`() {
        given(mediator.isBootstrapPresent()).thenReturn(false)
        context.updateSavepoint()
        verify(mediator, times(1)).isBootstrapPresent()
        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is not present but savepoint mode is not automatic`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(null)
        given(mediator.getSavepointMode()).thenReturn("NONE")
        context.updateSavepoint()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).getSavepointMode()
        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is not present and savepoint mode is automatic but last savepoint is recent`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(null)
        given(mediator.getSavepointMode()).thenReturn("AUTOMATIC")
        given(mediator.timeSinceLastSavepointRequestInSeconds()).thenReturn(30)
        given(mediator.getSavepointInterval()).thenReturn(60)
        context.updateSavepoint()
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).getSavepointMode()
        verify(mediator, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(mediator, times(1)).getSavepointInterval()
        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is not present and savepoint mode is automatic and last savepoint is not recent but savepoint request can't be created`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(null)
        given(mediator.getSavepointMode()).thenReturn("AUTOMATIC")
        given(mediator.timeSinceLastSavepointRequestInSeconds()).thenReturn(90)
        given(mediator.getSavepointInterval()).thenReturn(60)
        given(mediator.getSavepointOtions()).thenReturn(savepointOptions)
        given(mediator.triggerSavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.ERROR, savepointRequest))
        context.updateSavepoint()
        verify(mediator, times(1)).clusterSelector
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).getSavepointMode()
        verify(mediator, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(mediator, times(1)).getSavepointInterval()
        verify(mediator, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
        verify(mediator, times(1)).resetSavepointRequest()
//        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `updateSavepoint should change status when bootstrap is present and savepoint request is not present and savepoint mode is automatic and last savepoint is not recent`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(null)
        given(mediator.getSavepointMode()).thenReturn("AUTOMATIC")
        given(mediator.timeSinceLastSavepointRequestInSeconds()).thenReturn(90)
        given(mediator.getSavepointInterval()).thenReturn(60)
        given(mediator.getSavepointOtions()).thenReturn(savepointOptions)
        given(mediator.triggerSavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, savepointRequest))
        context.updateSavepoint()
        verify(mediator, times(1)).clusterSelector
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).getSavepointMode()
        verify(mediator, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(mediator, times(1)).getSavepointInterval()
        verify(mediator, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
        verify(mediator, times(1)).setSavepointRequest(eq(savepointRequest))
//        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is present but savepoint is not completed`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(savepointRequest)
        given(mediator.timeSinceLastUpdateInSeconds()).thenReturn(60)
        given(mediator.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        context.updateSavepoint()
        verify(mediator, times(1)).clusterSelector
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(mediator, times(1)).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is present but savepoint can't be completed`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(savepointRequest)
        given(mediator.timeSinceLastUpdateInSeconds()).thenReturn(60)
        given(mediator.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.ERROR, null))
        context.updateSavepoint()
        verify(mediator, times(1)).clusterSelector
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(mediator, times(1)).resetSavepointRequest()
        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `updateSavepoint should change status when bootstrap is present and savepoint request is present and savepoint has been completed`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(savepointRequest)
        given(mediator.timeSinceLastUpdateInSeconds()).thenReturn(60)
        given(mediator.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, "/tmp/1"))
        context.updateSavepoint()
        verify(mediator, times(1)).clusterSelector
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(mediator, times(1)).resetSavepointRequest()
        verify(mediator, times(1)).setSavepointPath(eq("/tmp/1"))
        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `updateSavepoint should change status when bootstrap is present and savepoint request is present and savepoint has timed out`() {
        given(mediator.isBootstrapPresent()).thenReturn(true)
        given(mediator.getSavepointRequest()).thenReturn(savepointRequest)
        given(mediator.timeSinceLastUpdateInSeconds()).thenReturn(600)
        given(mediator.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        context.updateSavepoint()
        verify(mediator, times(1)).clusterSelector
        verify(mediator, times(1)).isBootstrapPresent()
        verify(mediator, times(1)).getSavepointRequest()
        verify(mediator, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(mediator, times(1)).timeSinceLastUpdateInSeconds()
        verify(mediator, times(1)).resetSavepointRequest()
        verifyNoMoreInteractions(mediator)
    }
}
