package com.nextbreakpoint.flinkoperator.mediator.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterScale
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.DeleteOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskController
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.apache.log4j.Logger
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class TaskContextTest {
    private val clusterSelector = ClusterSelector(name = "test", namespace = "flink", uuid = "123")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val savepointOptions = SavepointOptions(targetPath = "file:///tmp")
    private val jobmanagerDeleteOptions = DeleteOptions(label = "role", value = "jobmanager", limit = 1)
    private val taskmanagerDeleteOptions = DeleteOptions(label = "role", value = "taskmanager", limit = 2)
    private val clusterScale = ClusterScale(taskManagers = 2, taskSlots = 1)
    private val logger = mock(Logger::class.java)
    private val taskController = mock(TaskController::class.java)
    private val taskContext = TaskContext(logger, taskController)

    @BeforeEach
    fun configure() {
        given(taskController.clusterSelector).thenReturn(clusterSelector)
        given(taskController.hasFinalizer()).thenReturn(true)
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.isJobFinished(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        given(taskController.isJobRunning(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.isJobFailed(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        given(taskController.arePodsRunning(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.getJobManagerReplicas()).thenReturn(1)
        given(taskController.getTaskManagerReplicas()).thenReturn(2)
        given(taskController.getClusterScale()).thenReturn(clusterScale)
        given(taskController.doesBootstrapJobExists()).thenReturn(true)
        given(taskController.doesServiceExists()).thenReturn(true)
        given(taskController.doesJobManagerPodsExists()).thenReturn(true)
        given(taskController.doesTaskManagerPodsExists()).thenReturn(true)
        given(taskController.timeSinceLastUpdateInSeconds()).thenReturn(2L)
        given(taskController.timeSinceLastSavepointRequestInSeconds()).thenReturn(30L)
        given(taskController.createBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.getSavepointRequest()).thenReturn(null)
        given(taskController.getSavepointOptions()).thenReturn(savepointOptions)
    }

    @Test
    fun `should remove finalizer`() {
        taskContext.removeFinalizer()
        verify(taskController, times(1)).removeFinalizer()
    }

    @Test
    fun `should change status on task timed out event`() {
        taskContext.onTaskTimeOut()
        verify(taskController, times(1)).setClusterStatus(any())
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Failed))
    }

    @Test
    fun `should change status on cluster terminated event`() {
        taskContext.onClusterTerminated()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Terminated))
    }

    @Test
    fun `should change status on cluster suspended event`() {
        taskContext.onClusterSuspended()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Suspended))
    }

    @Test
    fun `should change status on cluster started event`() {
        taskContext.onClusterStarted()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Running))
    }

    @Test
    fun `should change status on resource diverged event`() {
        taskContext.onResourceDiverged()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Restarting))
    }

    @Test
    fun `should change status on job cancelled event`() {
        taskContext.onClusterReadyToStop()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Stopping))
    }

    @Test
    fun `should change status on job finished event`() {
        taskContext.onJobFinished()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Finished))
    }

    @Test
    fun `should change status on job failed event`() {
        taskContext.onJobFailed()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Failed))
    }

    @Test
    fun `should change status on resource initialized event`() {
        taskContext.onResourceInitialise()
        verify(taskController, times(1)).initializeAnnotations()
        verify(taskController, times(1)).initializeStatus()
        verify(taskController, times(1)).updateDigests()
        verify(taskController, times(1)).addFinalizer()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Starting))
    }

    @Test
    fun `should change status on resource changed event`() {
        taskContext.onResourceChanged()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Restarting))
    }

    @Test
    fun `should change status on resource deleted event`() {
        taskContext.onResourceDeleted()
        verify(taskController, times(1)).setDeleteResources(true)
        verify(taskController, times(1)).resetManualAction()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Stopping))
    }

    @Test
    fun `should change status on resource ready to update event`() {
        taskContext.onClusterReadyToUpdate()
        verify(taskController, times(1)).updateDigests()
        verify(taskController, times(1)).updateStatus()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Starting))
    }

    @Test
    fun `should change status on resource ready to restart event`() {
        taskContext.onClusterReadyToRestart()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Updating))
    }

    @Test
    fun `should change status on cluster ready to scale up event`() {
        given(taskController.getTaskManagers()).thenReturn(2)
        taskContext.onClusterReadyToScale()
        verify(taskController, times(1)).rescaleCluster()
        verify(taskController, times(1)).getTaskManagers()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Starting))
    }

    @Test
    fun `should change status on cluster ready to scale down event`() {
        given(taskController.getTaskManagers()).thenReturn(0)
        taskContext.onClusterReadyToScale()
        verify(taskController, times(1)).rescaleCluster()
        verify(taskController, times(1)).getTaskManagers()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Stopping))
    }

    @Test
    fun `should change status on resource scaled event`() {
        taskContext.onResourceScaled()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Scaling))
    }

    @Test
    fun `cancelJob should return true when jobmanager service is not present`() {
        given(taskController.doesServiceExists()).thenReturn(false)
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).doesServiceExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when jobmanager pod is not present`() {
        given(taskController.doesJobManagerPodsExists()).thenReturn(false)
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when taskmanager pod is not present`() {
        given(taskController.doesTaskManagerPodsExists()).thenReturn(false)
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when pods are not running`() {
        given(taskController.arePodsRunning(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).arePodsRunning(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when there is an error checking pods running and job hasn't been stopped`() {
        given(taskController.arePodsRunning(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        given(taskController.stopJob(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).arePodsRunning(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is not required and job has been stopped`() {
        given(taskController.isSavepointRequired()).thenReturn(false)
        given(taskController.stopJob(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).isSavepointRequired()
        verify(taskController, times(1)).stopJob(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return false when savepoint is not required and job hasn't been stopped`() {
        given(taskController.isSavepointRequired()).thenReturn(false)
        given(taskController.stopJob(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).isSavepointRequired()
        verify(taskController, times(1)).stopJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is present but savepoint is not completed`() {
        given(taskController.isSavepointRequired()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(savepointRequest)
        given(taskController.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).isSavepointRequired()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).querySavepoint(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is present but savepoint can't be completed`() {
        given(taskController.isSavepointRequired()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(savepointRequest)
        given(taskController.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.ERROR, null))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).isSavepointRequired()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).querySavepoint(any(), any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is present but savepoint takes too long`() {
        given(taskController.isSavepointRequired()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(savepointRequest)
        given(taskController.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.timeSinceLastUpdateInSeconds()).thenReturn(301)
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).isSavepointRequired()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).querySavepoint(any(), any())
        verify(taskController, times(1)).timeSinceLastUpdateInSeconds()
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is present and savepoint is completed`() {
        given(taskController.isSavepointRequired()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(savepointRequest)
        given(taskController.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, "/tmp/1"))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).resetSavepointRequest()
        verify(taskController, times(1)).setSavepointPath("/tmp/1")
        verify(taskController, times(1)).querySavepoint(any(), any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is not present and job can't be cancelled`() {
        given(taskController.isSavepointRequired()).thenReturn(true)
        given(taskController.cancelJob(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).isSavepointRequired()
        verify(taskController, times(1)).cancelJob(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is not present and job has been cancelled`() {
        given(taskController.isSavepointRequired()).thenReturn(true)
        given(taskController.cancelJob(any(), any())).thenReturn(OperationResult(OperationStatus.OK, savepointRequest))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).setSavepointRequest(savepointRequest)
        verify(taskController, times(1)).isSavepointRequired()
        verify(taskController, times(1)).cancelJob(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is not present and job has been stopped`() {
        given(taskController.isSavepointRequired()).thenReturn(true)
        given(taskController.cancelJob(any(), any())).thenReturn(OperationResult(OperationStatus.OK, SavepointRequest("", "")))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).isSavepointRequired()
        verify(taskController, times(1)).cancelJob(any(), any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is not present and there is an error cancelling the job`() {
        given(taskController.isSavepointRequired()).thenReturn(true)
        given(taskController.cancelJob(any(), any())).thenReturn(OperationResult(OperationStatus.ERROR, null))
        val result = taskContext.cancelJob()
        verify(taskController, times(1)).isSavepointRequired()
        verify(taskController, times(1)).cancelJob(any(), any())
        assertThat(result).isTrue()
    }

    @Test
    fun `startCluster should return false when jobmanager service is not present`() {
        given(taskController.doesServiceExists()).thenReturn(false)
        val result = taskContext.startCluster()
        verify(taskController, times(1)).doesServiceExists()
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when jobmanager pod is not present`() {
        given(taskController.doesJobManagerPodsExists()).thenReturn(false)
        val result = taskContext.startCluster()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when taskmanager pod is not present`() {
        given(taskController.doesTaskManagerPodsExists()).thenReturn(false)
        val result = taskContext.startCluster()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap in not defined and there is an error checking cluster is ready`() {
        given(taskController.isBootstrapPresent()).thenReturn(false)
        given(taskController.isClusterReady(any(), eq(clusterScale))).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).isClusterReady(any(), eq(clusterScale))
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap in not defined and cluster is not ready`() {
        given(taskController.isBootstrapPresent()).thenReturn(false)
        given(taskController.isClusterReady(any(), eq(clusterScale))).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).isClusterReady(any(), eq(clusterScale))
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return true when bootstrap in not defined and cluster is ready`() {
        given(taskController.isBootstrapPresent()).thenReturn(false)
        given(taskController.isClusterReady(any(), eq(clusterScale))).thenReturn(OperationResult(OperationStatus.OK, true))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).isClusterReady(any(), eq(clusterScale))
        assertThat(result).isTrue()
    }

    @Test
    fun `startCluster should return false when bootstrap job is present and there is an error when listing jobs`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.isJobRunning(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).isJobRunning(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is present but job is not running`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.isJobRunning(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).isJobRunning(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return true when bootstrap job is present and job is running`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.isJobRunning(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).isJobRunning(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and there is an error checking readiness`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.isClusterReady(any(), eq(clusterScale))).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).isClusterReady(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is not ready`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.isClusterReady(any(), eq(clusterScale))).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).isClusterReady(any(), any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is ready but jar hasn't been removed`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.isClusterReady(any(), eq(clusterScale))).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.removeJar(any())).thenReturn(OperationResult(OperationStatus.ERROR, null))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).isClusterReady(any(), any())
        verify(taskController, times(1)).removeJar(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is ready and jar has been removed but there is an error stopping job`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.isClusterReady(any(), eq(clusterScale))).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.removeJar(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.stopJob(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).isClusterReady(any(), any())
        verify(taskController, times(1)).removeJar(any())
        verify(taskController, times(1)).stopJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is ready and jar has been removed but job hasn't been stopped`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.isClusterReady(any(), eq(clusterScale))).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.removeJar(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.stopJob(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).isClusterReady(any(), any())
        verify(taskController, times(1)).removeJar(any())
        verify(taskController, times(1)).stopJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is ready and jar has been removed and job has been stopped but bootstrap job hasn't been created`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.isClusterReady(any(), eq(clusterScale))).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.removeJar(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.stopJob(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.createBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.ERROR, "test"))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).isClusterReady(any(), any())
        verify(taskController, times(1)).removeJar(any())
        verify(taskController, times(1)).stopJob(any())
        verify(taskController, times(1)).createBootstrapJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when bootstrap job is not present and cluster is ready and jar has been removed and job has been stopped and bootstrap job has ben created`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.isClusterReady(any(), eq(clusterScale))).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.removeJar(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.stopJob(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.createBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.OK, "test"))
        val result = taskContext.startCluster()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).isClusterReady(any(), any())
        verify(taskController, times(1)).removeJar(any())
        verify(taskController, times(1)).stopJob(any())
        verify(taskController, times(1)).createBootstrapJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `suspendCluster should return false when pods haven't been stopped`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = taskContext.suspendCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).deletePods(any(), eq(jobmanagerDeleteOptions))
        verify(taskController, times(1)).deletePods(any(), eq(taskmanagerDeleteOptions))
        assertThat(result).isFalse()
    }

    @Test
    fun `suspendCluster should return false when there is an error checking pods are running`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = taskContext.suspendCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).deletePods(any(), eq(jobmanagerDeleteOptions))
        verify(taskController, times(1)).deletePods(any(), eq(taskmanagerDeleteOptions))
        assertThat(result).isFalse()
    }

    @Test
    fun `suspendCluster should return false when pods have been stopped but jobmanager service is present`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.deleteService(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.doesServiceExists()).thenReturn(true)
        val result = taskContext.suspendCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).deleteService(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `suspendCluster should return true when pods have been stopped and jobmanager service is not present and bootstrap job is not present`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.doesServiceExists()).thenReturn(false)
        val result = taskContext.suspendCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).doesServiceExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `terminateCluster should return false when pods haven't been stopped`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = taskContext.terminateCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).deletePods(any(), eq(jobmanagerDeleteOptions))
        verify(taskController, times(1)).deletePods(any(), eq(taskmanagerDeleteOptions))
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when there is an error checking pods are running`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = taskContext.terminateCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).deletePods(any(), eq(jobmanagerDeleteOptions))
        verify(taskController, times(1)).deletePods(any(), eq(taskmanagerDeleteOptions))
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when pods have been stopped but bootstrap job is present`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.deleteBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.doesBootstrapJobExists()).thenReturn(true)
        given(taskController.doesServiceExists()).thenReturn(false)
        given(taskController.doesJobManagerPodsExists()).thenReturn(false)
        given(taskController.doesTaskManagerPodsExists()).thenReturn(false)
        val result = taskContext.terminateCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).deleteBootstrapJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when pods have been stopped but jobmanager service is present`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.deleteService(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.doesServiceExists()).thenReturn(true)
        given(taskController.doesJobManagerPodsExists()).thenReturn(false)
        given(taskController.doesTaskManagerPodsExists()).thenReturn(false)
        val result = taskContext.terminateCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        verify(taskController, times(1)).deleteService(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when pods have been stopped but jobmanager pod is present`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.deletePods(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.doesServiceExists()).thenReturn(false)
        given(taskController.doesJobManagerPodsExists()).thenReturn(true)
        given(taskController.doesTaskManagerPodsExists()).thenReturn(false)
        val result = taskContext.terminateCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        verify(taskController, times(1)).deletePods(any(), eq(jobmanagerDeleteOptions))
        verify(taskController, times(1)).deletePods(any(), eq(taskmanagerDeleteOptions))
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return false when pods have been stopped but taskmanager pod is present`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.deletePods(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.doesServiceExists()).thenReturn(false)
        given(taskController.doesJobManagerPodsExists()).thenReturn(false)
        given(taskController.doesTaskManagerPodsExists()).thenReturn(true)
        val result = taskContext.terminateCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        verify(taskController, times(1)).deletePods(any(), eq(jobmanagerDeleteOptions))
        verify(taskController, times(1)).deletePods(any(), eq(taskmanagerDeleteOptions))
        assertThat(result).isFalse()
    }

    @Test
    fun `terminateCluster should return true when pods have been stopped and jobmanager service is not present and bootstrap job is not present and pods are not present and pvcs are not present`() {
        given(taskController.arePodsTerminated(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        given(taskController.doesServiceExists()).thenReturn(false)
        given(taskController.doesJobManagerPodsExists()).thenReturn(false)
        given(taskController.doesTaskManagerPodsExists()).thenReturn(false)
        val result = taskContext.terminateCluster()
        verify(taskController, times(1)).arePodsTerminated(any())
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `resetCluster should return false when bootstrap job is present`() {
        given(taskController.deleteBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.doesBootstrapJobExists()).thenReturn(true)
        val result = taskContext.resetCluster()
        verify(taskController, times(1)).doesBootstrapJobExists()
        verify(taskController, times(1)).deleteBootstrapJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `resetCluster should return true when bootstrap job is not present`() {
        given(taskController.deleteBootstrapJob(any())).thenReturn(OperationResult(OperationStatus.OK, null))
        given(taskController.doesBootstrapJobExists()).thenReturn(false)
        val result = taskContext.resetCluster()
        verify(taskController, times(1)).doesBootstrapJobExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when jobmanager service is not present`() {
        given(taskController.doesServiceExists()).thenReturn(false)
        val result = taskContext.hasResourceDiverged()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when jobmanager pod is not present`() {
        given(taskController.doesJobManagerPodsExists()).thenReturn(false)
        val result = taskContext.hasResourceDiverged()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when taskmanager pod is not present`() {
        given(taskController.doesTaskManagerPodsExists()).thenReturn(false)
        val result = taskContext.hasResourceDiverged()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when jobmanager replicas is not equals to desired value`() {
        given(taskController.getJobManagerReplicas()).thenReturn(0)
        val result = taskContext.hasResourceDiverged()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        verify(taskController, times(1)).getClusterScale()
        verify(taskController, times(1)).getJobManagerReplicas()
        verify(taskController, times(1)).getTaskManagerReplicas()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when taskmanager replicas is not equals to desired value`() {
        given(taskController.getTaskManagerReplicas()).thenReturn(0)
        val result = taskContext.hasResourceDiverged()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        verify(taskController, times(1)).getClusterScale()
        verify(taskController, times(1)).getJobManagerReplicas()
        verify(taskController, times(1)).getTaskManagerReplicas()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return false when jobmanager and taskmanager replicas are equal to desired value`() {
        val result = taskContext.hasResourceDiverged()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).doesJobManagerPodsExists()
        verify(taskController, times(1)).doesTaskManagerPodsExists()
        verify(taskController, times(1)).getClusterScale()
        verify(taskController, times(1)).getJobManagerReplicas()
        verify(taskController, times(1)).getTaskManagerReplicas()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasResourceChanged should return true when resource has changed`() {
        given(taskController.computeChanges()).thenReturn(listOf("TEST"))
        val result = taskContext.hasResourceChanged()
        verify(taskController, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceChanged should return false when resource hasn't changed`() {
        given(taskController.computeChanges()).thenReturn(listOf())
        val result = taskContext.hasResourceChanged()
        verify(taskController, times(1)).computeChanges()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFinished should return true when bootstrap is defined and job has finished`() {
        given(taskController.isJobFinished(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        val result = taskContext.hasJobFinished()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).isJobFinished(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `hasJobFinished should return false when bootstrap is defined and job hasn't finished`() {
        given(taskController.isJobFinished(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = taskContext.hasJobFinished()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).isJobFinished(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFinished should return false when bootstrap is defined and there is an error checking job has finished`() {
        given(taskController.isJobFinished(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = taskContext.hasJobFinished()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).isJobFinished(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFinished should return false when bootstrap is not defined`() {
        given(taskController.isBootstrapPresent()).thenReturn(false)
        val result = taskContext.hasJobFinished()
        verify(taskController, times(1)).isBootstrapPresent()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFailed should return true when bootstrap is defined and job has failed`() {
        given(taskController.isJobFailed(any())).thenReturn(OperationResult(OperationStatus.OK, true))
        val result = taskContext.hasJobFailed()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).isJobFailed(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `hasJobFailed should return false when bootstrap is defined and job hasn't failed`() {
        given(taskController.isJobFailed(any())).thenReturn(OperationResult(OperationStatus.OK, false))
        val result = taskContext.hasJobFailed()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).isJobFailed(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFailed should return false when bootstrap is defined and there is an error checking job has failed`() {
        given(taskController.isJobFailed(any())).thenReturn(OperationResult(OperationStatus.ERROR, false))
        val result = taskContext.hasJobFailed()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).isJobFailed(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `hasJobFailed should return false when bootstrap is not defined`() {
        given(taskController.isBootstrapPresent()).thenReturn(false)
        val result = taskContext.hasJobFailed()
        verify(taskController, times(1)).isBootstrapPresent()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasTaskTimedOut should return false when task hasn't timed out`() {
        given(taskController.timeSinceLastUpdateInSeconds()).thenReturn(10L)
        val result = taskContext.hasTaskTimedOut()
        verify(taskController, times(1)).timeSinceLastUpdateInSeconds()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasTaskTimedOut should return true when task has timed out`() {
        given(taskController.timeSinceLastUpdateInSeconds()).thenReturn(301L)
        val result = taskContext.hasTaskTimedOut()
        verify(taskController, times(1)).timeSinceLastUpdateInSeconds()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasScaleChanged should return false when scale hasn't changed`() {
        given(taskController.getDesiredTaskManagers()).thenReturn(2)
        given(taskController.getTaskManagers()).thenReturn(2)
        val result = taskContext.hasScaleChanged()
        verify(taskController, times(1)).getDesiredTaskManagers()
        verify(taskController, times(1)).getTaskManagers()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasScaleChanged should return true when scale has changed`() {
        given(taskController.getDesiredTaskManagers()).thenReturn(2)
        given(taskController.getTaskManagers()).thenReturn(1)
        val result = taskContext.hasScaleChanged()
        verify(taskController, times(1)).getDesiredTaskManagers()
        verify(taskController, times(1)).getTaskManagers()
        assertThat(result).isTrue()
    }

    @Test
    fun `isManualActionPresent should return true when manual action is not none`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.START)
        val result = taskContext.isManualActionPresent()
        verify(taskController, times(1)).getManualAction()
        assertThat(result).isTrue()
    }

    @Test
    fun `isManualActionPresent should return false when manual action is none`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.NONE)
        val result = taskContext.isManualActionPresent()
        verify(taskController, times(1)).getManualAction()
        assertThat(result).isFalse()
    }

    @Test
    fun `isResourceDeleted should return false when resource hasn't been deleted`() {
        given(taskController.hasBeenDeleted()).thenReturn(false)
        val result = taskContext.isResourceDeleted()
        verify(taskController, times(1)).hasBeenDeleted()
        assertThat(result).isFalse()
    }

    @Test
    fun `isResourceDeleted should return true when resource has been deleted`() {
        given(taskController.hasBeenDeleted()).thenReturn(true)
        val result = taskContext.isResourceDeleted()
        verify(taskController, times(1)).hasBeenDeleted()
        assertThat(result).isTrue()
    }

    @Test
    fun `shouldRestartJob should return false when restart policy is not always`() {
        given(taskController.getRestartPolicy()).thenReturn("NEVER")
        val result = taskContext.shouldRestart()
        verify(taskController, times(1)).getRestartPolicy()
        assertThat(result).isFalse()
    }

    @Test
    fun `shouldRestartJob should return true when restart policy is always`() {
        given(taskController.getRestartPolicy()).thenReturn("ALWAYS")
        val result = taskContext.shouldRestart()
        verify(taskController, times(1)).getRestartPolicy()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustTerminateResources should return false when resources don't have to be deleted`() {
        given(taskController.isDeleteResources()).thenReturn(false)
        val result = taskContext.mustTerminateResources()
        verify(taskController, times(1)).isDeleteResources()
        assertThat(result).isFalse()
    }

    @Test
    fun `mustTerminateResources should return true when resources have to be deleted`() {
        given(taskController.isDeleteResources()).thenReturn(true)
        val result = taskContext.mustTerminateResources()
        verify(taskController, times(1)).isDeleteResources()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustRecreateResources should return false when only bootstrap changed`() {
        given(taskController.computeChanges()).thenReturn(listOf("BOOTSTRAP"))
        val result = taskContext.mustRecreateResources()
        verify(taskController, times(1)).computeChanges()
        assertThat(result).isFalse()
    }

    @Test
    fun `mustRecreateResources should return true when jobmanager spec changed`() {
        given(taskController.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        val result = taskContext.mustRecreateResources()
        verify(taskController, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustRecreateResources should return true when taskmanager spec changed`() {
        given(taskController.computeChanges()).thenReturn(listOf("TASK_MANAGER"))
        val result = taskContext.mustRecreateResources()
        verify(taskController, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustRecreateResources should return true when runtime spec changed`() {
        given(taskController.computeChanges()).thenReturn(listOf("RUNTIME"))
        val result = taskContext.mustRecreateResources()
        verify(taskController, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `ensureServiceExist should do nothing when jobmanager service exists`() {
        given(taskController.doesServiceExists()).thenReturn(true)
        taskContext.ensureServiceExist()
        verify(taskController, times(1)).doesServiceExists()
    }

    @Test
    fun `ensureServiceExist should create resource when jobmanager service doesn't exists`() {
        given(taskController.doesServiceExists()).thenReturn(false)
        given(taskController.createService(any())).thenReturn(OperationResult(OperationStatus.OK, "test"))
        taskContext.ensureServiceExist()
        verify(taskController, times(1)).doesServiceExists()
        verify(taskController, times(1)).createService(eq(clusterSelector))
    }

    @Test
    fun `ensurePodsExists should do nothing when replicas are correct`() {
        given(taskController.getClusterScale()).thenReturn(clusterScale)
        given(taskController.getJobManagerReplicas()).thenReturn(1)
        given(taskController.getTaskManagerReplicas()).thenReturn(2)
        taskContext.ensurePodsExists()
        verify(taskController, times(1)).getClusterScale()
        verify(taskController, times(1)).getJobManagerReplicas()
        verify(taskController, times(1)).getTaskManagerReplicas()
    }

    @Test
    fun `ensurePodsExists create pods when jobmanager replicas is incorrect`() {
        given(taskController.getClusterScale()).thenReturn(clusterScale)
        given(taskController.getJobManagerReplicas()).thenReturn(0)
        given(taskController.getTaskManagerReplicas()).thenReturn(2)
        given(taskController.createJobManagerPods(any(), anyInt())).thenReturn(OperationResult(OperationStatus.OK, setOf("test")))
        taskContext.ensurePodsExists()
        verify(taskController, times(1)).getClusterScale()
        verify(taskController, times(1)).getJobManagerReplicas()
        verify(taskController, times(1)).getTaskManagerReplicas()
        verify(taskController, times(1)).createJobManagerPods(eq(clusterSelector), eq(1))
    }

    @Test
    fun `ensurePodsExists create pods when taskmanager replicas is incorrect`() {
        given(taskController.getClusterScale()).thenReturn(clusterScale)
        given(taskController.getJobManagerReplicas()).thenReturn(1)
        given(taskController.getTaskManagerReplicas()).thenReturn(1)
        given(taskController.createTaskManagerPods(any(), anyInt())).thenReturn(OperationResult(OperationStatus.OK, setOf("test")))
        taskContext.ensurePodsExists()
        verify(taskController, times(1)).getClusterScale()
        verify(taskController, times(1)).getJobManagerReplicas()
        verify(taskController, times(1)).getTaskManagerReplicas()
        verify(taskController, times(1)).createTaskManagerPods(eq(clusterSelector), eq(2))
    }

    @Test
    fun `executeManualAction should do nothing when manual action is none`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.NONE)
        taskContext.executeManualAction(setOf(ManualAction.NONE))
        verify(taskController, times(1)).getManualAction()
        verifyNoMoreInteractions(taskController)
    }

    @Test
    fun `executeManualAction should do nothing when manual action is start but action is not allowed`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.START)
        taskContext.executeManualAction(setOf())
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is stop but action is not allowed`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.STOP)
        taskContext.executeManualAction(setOf())
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is forget savepoint but action is not allowed`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.FORGET_SAVEPOINT)
        taskContext.executeManualAction(setOf())
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is trigger savepoint but action is not allowed`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        taskContext.executeManualAction(setOf())
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is trigger savepoint and bootstrap is not defined`() {
        given(taskController.isBootstrapPresent()).thenReturn(false)
        given(taskController.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        taskContext.executeManualAction(setOf(ManualAction.TRIGGER_SAVEPOINT))
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is start`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.START)
        taskContext.executeManualAction(setOf(ManualAction.START))
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Starting))
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is stop and job doesn't need to be cancelled`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.STOP)
        taskContext.executeManualAction(setOf(ManualAction.STOP), false)
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).setDeleteResources(eq(true))
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Stopping))
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is stop and job needs to be cancelled`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.STOP)
        taskContext.executeManualAction(setOf(ManualAction.STOP), true)
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).setClusterStatus(eq(ClusterStatus.Cancelling))
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is forget savepoint`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.FORGET_SAVEPOINT)
        taskContext.executeManualAction(setOf(ManualAction.FORGET_SAVEPOINT))
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).setSavepointPath(eq(""))
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is trigger savepoint and savepoint request is not present`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        given(taskController.getSavepointRequest()).thenReturn(null)
        given(taskController.triggerSavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, savepointRequest))
        taskContext.executeManualAction(setOf(ManualAction.TRIGGER_SAVEPOINT))
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
        verify(taskController, times(1)).setSavepointRequest(eq(savepointRequest))
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should not change status when manual action is trigger savepoint and savepoint request is present`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        given(taskController.getSavepointRequest()).thenReturn(savepointRequest)
        taskContext.executeManualAction(setOf(ManualAction.TRIGGER_SAVEPOINT))
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `executeManualAction should not change status when manual action is trigger savepoint and savepoint request can't be created`() {
        given(taskController.getManualAction()).thenReturn(ManualAction.TRIGGER_SAVEPOINT)
        given(taskController.getSavepointRequest()).thenReturn(null)
        given(taskController.triggerSavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.ERROR, savepointRequest))
        taskContext.executeManualAction(setOf(ManualAction.TRIGGER_SAVEPOINT))
        verify(taskController, times(1)).getManualAction()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).resetManualAction()
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is not present`() {
        given(taskController.isBootstrapPresent()).thenReturn(false)
        taskContext.updateSavepoint()
        verify(taskController, times(1)).isBootstrapPresent()
        verifyNoMoreInteractions(taskController)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is not present but savepoint mode is not automatic`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(null)
        given(taskController.getSavepointMode()).thenReturn("NONE")
        taskContext.updateSavepoint()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).getSavepointMode()
        verifyNoMoreInteractions(taskController)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is not present and savepoint mode is automatic but last savepoint is recent`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(null)
        given(taskController.getSavepointMode()).thenReturn("AUTOMATIC")
        given(taskController.timeSinceLastSavepointRequestInSeconds()).thenReturn(30)
        given(taskController.getSavepointInterval()).thenReturn(60)
        taskContext.updateSavepoint()
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).getSavepointMode()
        verify(taskController, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(taskController, times(1)).getSavepointInterval()
        verifyNoMoreInteractions(taskController)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is not present and savepoint mode is automatic and last savepoint is not recent but savepoint request can't be created`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(null)
        given(taskController.getSavepointMode()).thenReturn("AUTOMATIC")
        given(taskController.timeSinceLastSavepointRequestInSeconds()).thenReturn(90)
        given(taskController.getSavepointInterval()).thenReturn(60)
        given(taskController.getSavepointOptions()).thenReturn(savepointOptions)
        given(taskController.triggerSavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.ERROR, savepointRequest))
        taskContext.updateSavepoint()
        verify(taskController, times(1)).clusterSelector
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).getSavepointMode()
        verify(taskController, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(taskController, times(1)).getSavepointInterval()
        verify(taskController, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
        verify(taskController, times(1)).resetSavepointRequest()
//        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `updateSavepoint should change status when bootstrap is present and savepoint request is not present and savepoint mode is automatic and last savepoint is not recent`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(null)
        given(taskController.getSavepointMode()).thenReturn("AUTOMATIC")
        given(taskController.timeSinceLastSavepointRequestInSeconds()).thenReturn(90)
        given(taskController.getSavepointInterval()).thenReturn(60)
        given(taskController.getSavepointOptions()).thenReturn(savepointOptions)
        given(taskController.triggerSavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, savepointRequest))
        taskContext.updateSavepoint()
        verify(taskController, times(1)).clusterSelector
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).getSavepointMode()
        verify(taskController, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(taskController, times(1)).getSavepointInterval()
        verify(taskController, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
        verify(taskController, times(1)).setSavepointRequest(eq(savepointRequest))
//        verifyNoMoreInteractions(mediator)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is present but savepoint is not completed`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(savepointRequest)
        given(taskController.timeSinceLastUpdateInSeconds()).thenReturn(60)
        given(taskController.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        taskContext.updateSavepoint()
        verify(taskController, times(1)).clusterSelector
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(taskController, times(1)).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(taskController)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is present but savepoint can't be queried`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(savepointRequest)
        given(taskController.timeSinceLastUpdateInSeconds()).thenReturn(60)
        given(taskController.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.ERROR, null))
        taskContext.updateSavepoint()
        verify(taskController, times(1)).clusterSelector
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest))
        verifyNoMoreInteractions(taskController)
    }

    @Test
    fun `updateSavepoint should change status when bootstrap is present and savepoint request is present and savepoint has been completed`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(savepointRequest)
        given(taskController.timeSinceLastUpdateInSeconds()).thenReturn(60)
        given(taskController.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, "/tmp/1"))
        taskContext.updateSavepoint()
        verify(taskController, times(1)).clusterSelector
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(taskController, times(1)).resetSavepointRequest()
        verify(taskController, times(1)).setSavepointPath(eq("/tmp/1"))
        verifyNoMoreInteractions(taskController)
    }

    @Test
    fun `updateSavepoint should change status when bootstrap is present and savepoint request is present and savepoint has timed out`() {
        given(taskController.isBootstrapPresent()).thenReturn(true)
        given(taskController.getSavepointRequest()).thenReturn(savepointRequest)
        given(taskController.timeSinceLastUpdateInSeconds()).thenReturn(600)
        given(taskController.querySavepoint(any(), any())).thenReturn(OperationResult(OperationStatus.OK, null))
        taskContext.updateSavepoint()
        verify(taskController, times(1)).clusterSelector
        verify(taskController, times(1)).isBootstrapPresent()
        verify(taskController, times(1)).getSavepointRequest()
        verify(taskController, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest))
        verify(taskController, times(1)).timeSinceLastUpdateInSeconds()
        verify(taskController, times(1)).resetSavepointRequest()
        verifyNoMoreInteractions(taskController)
    }
}
