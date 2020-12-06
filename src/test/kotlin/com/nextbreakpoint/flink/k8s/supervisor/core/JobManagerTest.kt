package com.nextbreakpoint.flink.mediator.core

import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.RestartPolicy
import com.nextbreakpoint.flink.common.SavepointMode
import com.nextbreakpoint.flink.common.SavepointOptions
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.k8s.common.Timeout
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.supervisor.core.JobController
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager
import com.nextbreakpoint.flink.testing.KotlinMockito.any
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import org.apache.log4j.Logger
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class JobManagerTest {
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val savepointOptions = SavepointOptions(targetPath = "file:///tmp")
    private val logger = mock(Logger::class.java)
    private val controller = mock(JobController::class.java)
    private val manager = JobManager(logger, controller)

    @BeforeEach
    fun configure() {
        given(controller.namespace).thenReturn("test")
        given(controller.clusterName).thenReturn("test")
        given(controller.jobName).thenReturn("test")
        given(controller.hasFinalizer()).thenReturn(true)
        given(controller.hasJobId()).thenReturn(false)
        given(controller.doesBootstrapJobExists()).thenReturn(true)
        given(controller.isJobCancelled()).thenReturn(false)
        given(controller.isJobFinished()).thenReturn(false)
        given(controller.isJobFailed()).thenReturn(false)
        given(controller.isWithoutSavepoint()).thenReturn(false)
        given(controller.shouldCreateSavepoint()).thenReturn(true)
        given(controller.isClusterStopped()).thenReturn(false)
        given(controller.isClusterStopping()).thenReturn(false)
        given(controller.isClusterStarted()).thenReturn(true)
        given(controller.isClusterStarting()).thenReturn(false)
        given(controller.isClusterUpdated()).thenReturn(true)
        given(controller.timeSinceLastUpdateInSeconds()).thenReturn(2L)
        given(controller.timeSinceLastSavepointRequestInSeconds()).thenReturn(30L)
        given(controller.getSavepointRequest()).thenReturn(null)
        given(controller.getSavepointOptions()).thenReturn(savepointOptions)
    }

    @Test
    fun `should change status on job started event`() {
        manager.onJobStarted()
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Started))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updated))
    }

    @Test
    fun `should change status on job canceled event`() {
        manager.onJobCanceled()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Stopped))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updated))
    }

    @Test
    fun `should change status on job finished event`() {
        manager.onJobFinished()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Stopped))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updated))
    }

    @Test
    fun `should change status on job failed event`() {
        manager.onJobFailed()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Stopped))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updated))
    }

    @Test
    fun `should change status on job stopped event`() {
        manager.onJobStopped()
        verify(controller, times(1)).resetJob()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Stopped))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updated))
    }

    @Test
    fun `should change status on job terminated event`() {
        manager.onJobTerminated()
        verify(controller, times(1)).resetJob()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Terminated))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updated))
    }

    @Test
    fun `should change status on job ready to restart event`() {
        manager.onJobReadyToRestart()
        verify(controller, times(1)).updateStatus()
        verify(controller, times(1)).updateDigests()
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Starting))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on job aborted stopping event`() {
        manager.onJobAborted()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Stopping))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on cluster stopping event`() {
        manager.onClusterStopping()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Stopping))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on job unhealthy event`() {
        manager.onClusterUnhealthy()
        verify(controller, times(1)).resetJob()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Stopped))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updated))
    }

    @Test
    fun `should change status on resource initialized event`() {
        manager.onResourceInitialise()
        verify(controller, times(1)).initializeAnnotations()
        verify(controller, times(1)).initializeStatus()
        verify(controller, times(1)).updateDigests()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setClusterName(eq("test"))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Starting))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on resource deleted event`() {
        manager.onResourceDeleted()
        verify(controller, times(1)).resetAction()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setDeleteResources(true)
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Stopping))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on resource changed event`() {
        manager.onResourceChanged()
        verify(controller, times(1)).setShouldRestart(eq(true))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Stopping))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should update status on set resource updated`() {
        manager.setResourceUpdated(true)
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updated))
        manager.setResourceUpdated(false)
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should update status on set cluster health`() {
        manager.setClusterHealth("ok")
        verify(controller, times(1)).setClusterHealth(eq("ok"))
    }

    @Test
    fun `isClusterReady should return true when cluster is ready`() {
        given(controller.getRequiredTaskSlots()).thenReturn(2)
        given(controller.isClusterReady(eq(2))).thenReturn(Result(ResultStatus.OK, true))
        val result = manager.isClusterReady()
        verify(controller, times(1)).isClusterReady(eq(2))
        assertThat(result).isTrue()
    }

    @Test
    fun `isClusterReady should return false when cluster is not ready`() {
        given(controller.getRequiredTaskSlots()).thenReturn(2)
        given(controller.isClusterReady(eq(2))).thenReturn(Result(ResultStatus.OK, false))
        val result = manager.isClusterReady()
        verify(controller, times(1)).isClusterReady(eq(2))
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterReady should return false when there is an error`() {
        given(controller.getRequiredTaskSlots()).thenReturn(2)
        given(controller.isClusterReady(eq(2))).thenReturn(Result(ResultStatus.ERROR, true))
        val result = manager.isClusterReady()
        verify(controller, times(1)).isClusterReady(eq(2))
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterUnhealthy should return false when cluster is stopping`() {
        given(controller.isClusterStopping()).thenReturn(true)
        val result = manager.isClusterUnhealthy()
        verify(controller, times(1)).isClusterStopping()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterUnhealthy should return false when cluster is stopped`() {
        given(controller.isClusterStopped()).thenReturn(true)
        val result = manager.isClusterUnhealthy()
        verify(controller, times(1)).isClusterStopped()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterUnhealthy should return false when cluster is healthy`() {
        given(controller.isClusterHealthy()).thenReturn(Result(ResultStatus.OK, true))
        val result = manager.isClusterUnhealthy()
        verify(controller, times(1)).isClusterHealthy()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterUnhealthy should return false when cluster is not healthy but timeout is not expired yet`() {
        given(controller.isClusterHealthy()).thenReturn(Result(ResultStatus.OK, false))
        given(controller.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.TASK_TIMEOUT - 1)
        val result = manager.isClusterUnhealthy()
        verify(controller, times(1)).isClusterHealthy()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterUnhealthy should return true when cluster is not healthy and timeout is expired`() {
        given(controller.isClusterHealthy()).thenReturn(Result(ResultStatus.OK, false))
        given(controller.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.TASK_TIMEOUT + 1)
        val result = manager.isClusterUnhealthy()
        verify(controller, times(1)).isClusterHealthy()
        assertThat(result).isTrue()
    }

    @Test
    fun `isClusterUnhealthy should return false when there is an error but timeout is not expired yet`() {
        given(controller.isClusterHealthy()).thenReturn(Result(ResultStatus.ERROR, true))
        given(controller.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.TASK_TIMEOUT - 1)
        val result = manager.isClusterUnhealthy()
        verify(controller, times(1)).isClusterHealthy()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterUnhealthy should return true when there is an error and timeout is expired`() {
        given(controller.isClusterHealthy()).thenReturn(Result(ResultStatus.ERROR, true))
        given(controller.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.TASK_TIMEOUT + 1)
        val result = manager.isClusterUnhealthy()
        verify(controller, times(1)).isClusterHealthy()
        assertThat(result).isTrue()
    }

    @Test
    fun `startJob should return true when bootstrap job exists and job has id`() {
        given(controller.doesBootstrapJobExists()).thenReturn(true)
        given(controller.hasJobId()).thenReturn(true)
        val result = manager.startJob()
        verify(controller, times(1)).doesBootstrapJobExists()
        verify(controller, times(1)).hasJobId()
        assertThat(result).isTrue()
    }

    @Test
    fun `startJob should return false when bootstrap job exists but job doesn't have id`() {
        given(controller.doesBootstrapJobExists()).thenReturn(true)
        given(controller.hasJobId()).thenReturn(false)
        val result = manager.startJob()
        verify(controller, times(1)).doesBootstrapJobExists()
        verify(controller, times(1)).hasJobId()
        assertThat(result).isFalse()
    }

    @Test
    fun `startJob should remove job id when bootstrap job doesn't exit and job has id`() {
        given(controller.doesBootstrapJobExists()).thenReturn(false)
        given(controller.hasJobId()).thenReturn(true)
        val result = manager.startJob()
        verify(controller, times(1)).doesBootstrapJobExists()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).resetJob()
        assertThat(result).isFalse()
    }

    @Test
    fun `startJob should create bootstrap job when bootstrap job doesn't exit and job doesn't have id`() {
        given(controller.doesBootstrapJobExists()).thenReturn(false)
        given(controller.hasJobId()).thenReturn(false)
        given(controller.createBootstrapJob()).thenReturn(Result(ResultStatus.OK, null))
        val result = manager.startJob()
        verify(controller, times(1)).doesBootstrapJobExists()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).createBootstrapJob()
        assertThat(result).isFalse()
    }

    @Test
    fun `startJob should remove savepoint when savepoint is not required`() {
        given(controller.doesBootstrapJobExists()).thenReturn(false)
        given(controller.hasJobId()).thenReturn(false)
        given(controller.isWithoutSavepoint()).thenReturn(true)
        given(controller.createBootstrapJob()).thenReturn(Result(ResultStatus.OK, null))
        val result = manager.startJob()
        verify(controller, times(1)).isClusterUpdated()
        verify(controller, times(1)).doesBootstrapJobExists()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).setSavepointPath(eq(""))
        verify(controller, times(1)).createBootstrapJob()
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return true when job doesn't have id`() {
        given(controller.hasJobId()).thenReturn(false)
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should remove job id when savepoint is not required and job has been stopped`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.isWithoutSavepoint()).thenReturn(true)
        given(controller.stopJob()).thenReturn(Result(ResultStatus.OK, null))
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isDeleteResources()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).stopJob()
        verify(controller, times(1)).resetJob()
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should not remove job id when savepoint is not required and job hasn't been stopped`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.isWithoutSavepoint()).thenReturn(true)
        given(controller.stopJob()).thenReturn(Result(ResultStatus.ERROR, null))
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isDeleteResources()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).stopJob()
        verify(controller, times(0)).resetJob()
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is present but savepoint is not completed`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(savepointRequest)
        given(controller.querySavepoint(any())).thenReturn(Result(ResultStatus.OK, null))
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isDeleteResources()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).querySavepoint(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is present but savepoint can't be completed and job can't be stopped`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(savepointRequest)
        given(controller.querySavepoint(any())).thenReturn(Result(ResultStatus.ERROR, null))
        given(controller.stopJob()).thenReturn(Result(ResultStatus.ERROR, null))
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isDeleteResources()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).querySavepoint(any())
        verify(controller, times(1)).stopJob()
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is present but savepoint can't be completed`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(savepointRequest)
        given(controller.querySavepoint(any())).thenReturn(Result(ResultStatus.ERROR, null))
        given(controller.stopJob()).thenReturn(Result(ResultStatus.OK, null))
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isDeleteResources()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).querySavepoint(any())
        verify(controller, times(1)).stopJob()
        verify(controller, times(1)).resetJob()
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is present and savepoint is completed`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(savepointRequest)
        given(controller.querySavepoint(any())).thenReturn(Result(ResultStatus.OK, "/tmp/1"))
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isDeleteResources()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).querySavepoint(any())
        verify(controller, times(1)).resetSavepointRequest()
        verify(controller, times(1)).setSavepointPath("/tmp/1")
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is not present and job can't be cancelled`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.cancelJob(any())).thenReturn(Result(ResultStatus.OK, null))
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isDeleteResources()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).cancelJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is not present and job has been cancelled`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.cancelJob(any())).thenReturn(Result(ResultStatus.OK, savepointRequest))
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isDeleteResources()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).cancelJob(any())
        verify(controller, times(1)).setSavepointRequest(savepointRequest)
        assertThat(result).isFalse()
    }

    @Test
    fun `cancelJob should return true when savepoint is required and savepoint request is not present and job has been stopped`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.cancelJob(any())).thenReturn(Result(ResultStatus.OK, SavepointRequest("", "")))
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isDeleteResources()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).cancelJob(any())
        assertThat(result).isTrue()
    }

    @Test
    fun `cancelJob should return false when savepoint is required and savepoint request is not present and there is an error cancelling the job`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.cancelJob(any())).thenReturn(Result(ResultStatus.ERROR, null))
        val result = manager.cancelJob()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).isDeleteResources()
        verify(controller, times(1)).isWithoutSavepoint()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).cancelJob(any())
        assertThat(result).isFalse()
    }

    @Test
    fun `hasSpecificationChanged should return false when specification didn't change`() {
        given(controller.computeChanges()).thenReturn(listOf())
        val result = manager.hasSpecificationChanged()
        verify(controller, times(1)).computeChanges()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasSpecificationChanged should return true when specification changed`() {
        given(controller.computeChanges()).thenReturn(listOf("TEST"))
        val result = manager.hasSpecificationChanged()
        verify(controller, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `updateJobStatus should do nothing when job doesn't have id`() {
        given(controller.hasJobId()).thenReturn(false)
        manager.updateJobStatus()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(0)).getJobStatus()
    }

    @Test
    fun `updateJobStatus should update status when job has id`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getJobStatus()).thenReturn(Result(ResultStatus.OK, "TEST"))
        manager.updateJobStatus()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getJobStatus()
        verify(controller, times(1)).setJobStatus(eq("TEST"))
    }

    @Test
    fun `updateJobStatus should not update status when job has id but there ia an error`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getJobStatus()).thenReturn(Result(ResultStatus.ERROR, "TEST"))
        manager.updateJobStatus()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getJobStatus()
        verify(controller, times(0)).setJobStatus(any())
    }

    @Test
    fun `isJobCancelled should return true when job is canceled`() {
        given(controller.isJobCancelled()).thenReturn(true)
        val result = manager.isJobCancelled()
        verify(controller, times(1)).isJobCancelled()
        assertThat(result).isTrue()
    }

    @Test
    fun `isJobCancelled should return false when job is not canceled`() {
        given(controller.isJobCancelled()).thenReturn(false)
        val result = manager.isJobCancelled()
        verify(controller, times(1)).isJobCancelled()
        assertThat(result).isFalse()
    }

    @Test
    fun `isJobFinished should return true when job is canceled`() {
        given(controller.isJobFinished()).thenReturn(true)
        val result = manager.isJobFinished()
        verify(controller, times(1)).isJobFinished()
        assertThat(result).isTrue()
    }

    @Test
    fun `isJobFinished should return false when job is not canceled`() {
        given(controller.isJobFinished()).thenReturn(false)
        val result = manager.isJobFinished()
        verify(controller, times(1)).isJobFinished()
        assertThat(result).isFalse()
    }

    @Test
    fun `isJobFailed should return true when job is canceled`() {
        given(controller.isJobFailed()).thenReturn(true)
        val result = manager.isJobFailed()
        verify(controller, times(1)).isJobFailed()
        assertThat(result).isTrue()
    }

    @Test
    fun `isJobFailed should return false when job is not canceled`() {
        given(controller.isJobFailed()).thenReturn(false)
        val result = manager.isJobFailed()
        verify(controller, times(1)).isJobFailed()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasTaskTimedOut should return false when task hasn't timed out`() {
        given(controller.timeSinceLastUpdateInSeconds()).thenReturn(10L)
        val result = manager.hasTaskTimedOut()
        verify(controller, times(1)).timeSinceLastUpdateInSeconds()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasTaskTimedOut should return true when task has timed out`() {
        given(controller.timeSinceLastUpdateInSeconds()).thenReturn(301L)
        val result = manager.hasTaskTimedOut()
        verify(controller, times(1)).timeSinceLastUpdateInSeconds()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasParallelismChanged should return true when current parallelism is not equals to job parallelism`() {
        given(controller.getDeclaredJobParallelism()).thenReturn(1)
        given(controller.getCurrentJobParallelism()).thenReturn(2)
        val result = manager.hasParallelismChanged()
        verify(controller, times(1)).getDeclaredJobParallelism()
        verify(controller, times(1)).getCurrentJobParallelism()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasParallelismChanged should return false when current parallelism is equals to job parallelism`() {
        given(controller.getDeclaredJobParallelism()).thenReturn(2)
        given(controller.getCurrentJobParallelism()).thenReturn(2)
        val result = manager.hasParallelismChanged()
        verify(controller, times(1)).getDeclaredJobParallelism()
        verify(controller, times(1)).getCurrentJobParallelism()
        assertThat(result).isFalse()
    }

    @Test
    fun `isManualActionPresent should return true when manual action is not none`() {
        given(controller.getAction()).thenReturn(Action.START)
        val result = manager.isActionPresent()
        verify(controller, times(1)).getAction()
        assertThat(result).isTrue()
    }

    @Test
    fun `isManualActionPresent should return false when manual action is none`() {
        given(controller.getAction()).thenReturn(Action.NONE)
        val result = manager.isActionPresent()
        verify(controller, times(1)).getAction()
        assertThat(result).isFalse()
    }

    @Test
    fun `isResourceDeleted should return false when resource hasn't been deleted`() {
        given(controller.hasBeenDeleted()).thenReturn(false)
        val result = manager.isResourceDeleted()
        verify(controller, times(1)).hasBeenDeleted()
        assertThat(result).isFalse()
    }

    @Test
    fun `isResourceDeleted should return true when resource has been deleted`() {
        given(controller.hasBeenDeleted()).thenReturn(true)
        val result = manager.isResourceDeleted()
        verify(controller, times(1)).hasBeenDeleted()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustTerminateResources should return false when resources don't have to be deleted`() {
        given(controller.isDeleteResources()).thenReturn(false)
        val result = manager.mustTerminateResources()
        verify(controller, times(1)).isDeleteResources()
        assertThat(result).isFalse()
    }

    @Test
    fun `mustTerminateResources should return true when resources have to be deleted`() {
        given(controller.isDeleteResources()).thenReturn(true)
        val result = manager.mustTerminateResources()
        verify(controller, times(1)).isDeleteResources()
        assertThat(result).isTrue()
    }

    @Test
    fun `shouldRestartJob should return false when restart policy is not always`() {
        given(controller.getRestartPolicy()).thenReturn(RestartPolicy.Never)
        val result = manager.shouldRestartJob()
        verify(controller, times(2)).getRestartPolicy()
        assertThat(result).isFalse()
    }

    @Test
    fun `shouldRestartJob should return true when restart policy is always`() {
        given(controller.getRestartPolicy()).thenReturn(RestartPolicy.Always)
        val result = manager.shouldRestartJob()
        verify(controller, times(1)).getRestartPolicy()
        assertThat(result).isTrue()
    }

    @Test
    fun `shouldRestart should return true when restart is needed`() {
        given(controller.shouldRestart()).thenReturn(true)
        val result = manager.shouldRestart()
        verify(controller, times(1)).shouldRestart()
        assertThat(result).isTrue()
    }

    @Test
    fun `shouldRestart should return false when restart is not needed`() {
        given(controller.shouldRestart()).thenReturn(false)
        val result = manager.shouldRestart()
        verify(controller, times(1)).shouldRestart()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterStopped should return false when cluster is not stopped`() {
        given(controller.isClusterStopped()).thenReturn(false)
        val result = manager.isClusterStopped()
        verify(controller, times(1)).isClusterStopped()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterStopped should return true when cluster is stopped`() {
        given(controller.isClusterStopped()).thenReturn(true)
        val result = manager.isClusterStopped()
        verify(controller, times(1)).isClusterStopped()
        assertThat(result).isTrue()
    }

    @Test
    fun `isClusterStopping should return false when cluster is not stopping`() {
        given(controller.isClusterStopping()).thenReturn(false)
        val result = manager.isClusterStopping()
        verify(controller, times(1)).isClusterStopping()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterStopping should return true when cluster is stopping`() {
        given(controller.isClusterStopping()).thenReturn(true)
        val result = manager.isClusterStopping()
        verify(controller, times(1)).isClusterStopping()
        assertThat(result).isTrue()
    }

    @Test
    fun `isClusterStarted should return false when cluster is not started`() {
        given(controller.isClusterStarted()).thenReturn(false)
        val result = manager.isClusterStarted()
        verify(controller, times(1)).isClusterStarted()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterStarted should return true when cluster is started`() {
        given(controller.isClusterStarted()).thenReturn(true)
        val result = manager.isClusterStarted()
        verify(controller, times(1)).isClusterStarted()
        assertThat(result).isTrue()
    }

    @Test
    fun `isClusterStarting should return false when cluster is not starting`() {
        given(controller.isClusterStarting()).thenReturn(false)
        val result = manager.isClusterStarting()
        verify(controller, times(1)).isClusterStarting()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterStarting should return true when cluster is starting`() {
        given(controller.isClusterStarting()).thenReturn(true)
        val result = manager.isClusterStarting()
        verify(controller, times(1)).isClusterStarting()
        assertThat(result).isTrue()
    }

    @Test
    fun `isClusterUpdated should return false when cluster is not updated`() {
        given(controller.isClusterUpdated()).thenReturn(false)
        val result = manager.isClusterUpdated()
        verify(controller, times(1)).isClusterUpdated()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterUpdated should return true when cluster is updated`() {
        given(controller.isClusterUpdated()).thenReturn(true)
        val result = manager.isClusterUpdated()
        verify(controller, times(1)).isClusterUpdated()
        assertThat(result).isTrue()
    }

    @Test
    fun `terminateBootstrapJob should return true when bootstrap job doesn't exit`() {
        given(controller.doesBootstrapJobExists()).thenReturn(false)
        val result = manager.terminateBootstrapJob()
        verify(controller, times(1)).doesBootstrapJobExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `terminateBootstrapJob should return false when bootstrap job exits`() {
        given(controller.doesBootstrapJobExists()).thenReturn(true)
        given(controller.deleteBootstrapJob()).thenReturn(Result(ResultStatus.OK, null))
        val result = manager.terminateBootstrapJob()
        verify(controller, times(1)).doesBootstrapJobExists()
        verify(controller, times(1)).deleteBootstrapJob()
        assertThat(result).isFalse()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is none`() {
        given(controller.getAction()).thenReturn(Action.NONE)
        manager.executeAction(setOf(Action.NONE))
        verify(controller, times(1)).getAction()
        verifyNoMoreInteractions(controller)
    }

    @Test
    fun `executeManualAction should do nothing when manual action is start but action is not allowed`() {
        given(controller.getAction()).thenReturn(Action.START)
        manager.executeAction(setOf())
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is stop but action is not allowed`() {
        given(controller.getAction()).thenReturn(Action.STOP)
        manager.executeAction(setOf())
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is forget savepoint but action is not allowed`() {
        given(controller.getAction()).thenReturn(Action.FORGET_SAVEPOINT)
        manager.executeAction(setOf())
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is trigger savepoint but action is not allowed`() {
        given(controller.getAction()).thenReturn(Action.TRIGGER_SAVEPOINT)
        manager.executeAction(setOf())
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is trigger savepoint and job doesn't have id`() {
        given(controller.hasJobId()).thenReturn(false)
        given(controller.getAction()).thenReturn(Action.TRIGGER_SAVEPOINT)
        manager.executeAction(setOf(Action.TRIGGER_SAVEPOINT))
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is start`() {
        given(controller.getAction()).thenReturn(Action.START)
        manager.executeAction(setOf(Action.START))
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).updateStatus()
        verify(controller, times(1)).updateDigests()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Starting))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is stop`() {
        given(controller.getAction()).thenReturn(Action.STOP)
        manager.executeAction(setOf(Action.STOP))
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(JobStatus.Stopping))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is forget savepoint`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getAction()).thenReturn(Action.FORGET_SAVEPOINT)
        manager.executeAction(setOf(Action.FORGET_SAVEPOINT))
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).setSavepointPath(eq(""))
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is trigger savepoint and savepoint request is not present`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getAction()).thenReturn(Action.TRIGGER_SAVEPOINT)
        given(controller.getSavepointRequest()).thenReturn(null)
        given(controller.triggerSavepoint(any())).thenReturn(Result(ResultStatus.OK, savepointRequest))
        manager.executeAction(setOf(Action.TRIGGER_SAVEPOINT))
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).triggerSavepoint(eq(savepointOptions))
        verify(controller, times(1)).setSavepointRequest(eq(savepointRequest))
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should not change status when manual action is trigger savepoint and savepoint request is present`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getAction()).thenReturn(Action.TRIGGER_SAVEPOINT)
        given(controller.getSavepointRequest()).thenReturn(savepointRequest)
        manager.executeAction(setOf(Action.TRIGGER_SAVEPOINT))
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should not change status when manual action is trigger savepoint and savepoint request can't be created`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getAction()).thenReturn(Action.TRIGGER_SAVEPOINT)
        given(controller.getSavepointRequest()).thenReturn(null)
        given(controller.triggerSavepoint(any())).thenReturn(Result(ResultStatus.ERROR, savepointRequest))
        manager.executeAction(setOf(Action.TRIGGER_SAVEPOINT))
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `updateSavepoint should not change status when job doesn't have id`() {
        given(controller.hasJobId()).thenReturn(false)
        manager.updateSavepoint()
        verify(controller, times(1)).hasJobId()
        verifyNoMoreInteractions(controller)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is not present but savepoint mode is not automatic`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(null)
        given(controller.getSavepointMode()).thenReturn(SavepointMode.Manual)
        manager.updateSavepoint()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).getSavepointInterval()
        verifyNoMoreInteractions(controller)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is not present and savepoint mode is automatic but last savepoint is recent`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(null)
        given(controller.getSavepointMode()).thenReturn(SavepointMode.Automatic)
        given(controller.timeSinceLastSavepointRequestInSeconds()).thenReturn(30)
        given(controller.getSavepointInterval()).thenReturn(60)
        manager.updateSavepoint()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(controller, times(2)).getSavepointInterval()
        verifyNoMoreInteractions(controller)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is not present and savepoint mode is automatic and last savepoint is not recent but savepoint request can't be created`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(null)
        given(controller.getSavepointMode()).thenReturn(SavepointMode.Automatic)
        given(controller.timeSinceLastSavepointRequestInSeconds()).thenReturn(90)
        given(controller.getSavepointInterval()).thenReturn(60)
        given(controller.getSavepointOptions()).thenReturn(savepointOptions)
        given(controller.triggerSavepoint(any())).thenReturn(Result(ResultStatus.ERROR, savepointRequest))
        manager.updateSavepoint()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(controller, times(2)).getSavepointInterval()
        verify(controller, times(1)).getSavepointOptions()
        verify(controller, times(1)).triggerSavepoint(eq(savepointOptions))
        verifyNoMoreInteractions(controller)
    }

    @Test
    fun `updateSavepoint should change status when bootstrap is present and savepoint request is not present and savepoint mode is automatic and last savepoint is not recent`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(null)
        given(controller.getSavepointMode()).thenReturn(SavepointMode.Automatic)
        given(controller.timeSinceLastSavepointRequestInSeconds()).thenReturn(90)
        given(controller.getSavepointInterval()).thenReturn(60)
        given(controller.getSavepointOptions()).thenReturn(savepointOptions)
        given(controller.triggerSavepoint(any())).thenReturn(Result(ResultStatus.OK, savepointRequest))
        manager.updateSavepoint()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).timeSinceLastSavepointRequestInSeconds()
        verify(controller, times(2)).getSavepointInterval()
        verify(controller, times(1)).getSavepointOptions()
        verify(controller, times(1)).triggerSavepoint(eq(savepointOptions))
        verify(controller, times(1)).setSavepointRequest(eq(savepointRequest))
        verifyNoMoreInteractions(controller)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is present but savepoint is not completed`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(savepointRequest)
        given(controller.querySavepoint(any())).thenReturn(Result(ResultStatus.OK, null))
        manager.updateSavepoint()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).querySavepoint(eq(savepointRequest))
        verifyNoMoreInteractions(controller)
    }

    @Test
    fun `updateSavepoint should not change status when bootstrap is present and savepoint request is present but savepoint can't be queried`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(savepointRequest)
        given(controller.querySavepoint(any())).thenReturn(Result(ResultStatus.ERROR, null))
        manager.updateSavepoint()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).querySavepoint(eq(savepointRequest))
        verify(controller, times(1)).resetSavepointRequest()
        verifyNoMoreInteractions(controller)
    }

    @Test
    fun `updateSavepoint should change status when bootstrap is present and savepoint request is present and savepoint has been completed`() {
        given(controller.hasJobId()).thenReturn(true)
        given(controller.getSavepointRequest()).thenReturn(savepointRequest)
        given(controller.querySavepoint(any())).thenReturn(Result(ResultStatus.OK, "/tmp/1"))
        manager.updateSavepoint()
        verify(controller, times(1)).hasJobId()
        verify(controller, times(1)).getSavepointRequest()
        verify(controller, times(1)).querySavepoint(eq(savepointRequest))
        verify(controller, times(1)).resetSavepointRequest()
        verify(controller, times(1)).setSavepointPath(eq("/tmp/1"))
        verifyNoMoreInteractions(controller)
    }
}
