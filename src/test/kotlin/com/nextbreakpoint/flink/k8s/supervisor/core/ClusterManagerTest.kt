package com.nextbreakpoint.flink.mediator.core

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.DeleteOptions
import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterJobSpec
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterController
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager
import com.nextbreakpoint.flink.k8s.supervisor.core.Timeout
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

class ClusterManagerTest {
    private val clusterSelector = ResourceSelector(name = "test", namespace = "flink", uid = "123")
    private val logger = mock(Logger::class.java)
    private val controller = mock(ClusterController::class.java)
    private val manager = ClusterManager(logger, controller)

    @BeforeEach
    fun configure() {
        given(controller.clusterSelector).thenReturn(clusterSelector)
        given(controller.hasFinalizer()).thenReturn(true)
        given(controller.getJobManagerReplicas()).thenReturn(1)
        given(controller.getTaskManagerReplicas()).thenReturn(2)
        given(controller.haveJobsBeenCreated()).thenReturn(true)
        given(controller.doesJobManagerServiceExists()).thenReturn(true)
        given(controller.doesJobManagerPodExists()).thenReturn(true)
        given(controller.doesTaskManagerPodsExist()).thenReturn(true)
        given(controller.timeSinceLastUpdateInSeconds()).thenReturn(2L)
        given(controller.timeSinceLastRescaleInSeconds()).thenReturn(30L)
        given(controller.removeUnusedTaskManagers()).thenReturn(mapOf())
        given(controller.createJob(any<V1FlinkJob>())).thenReturn(Result(ResultStatus.OK, null))
        given(controller.deletePods(any())).thenReturn(Result(ResultStatus.OK, null))
        given(controller.deleteService()).thenReturn(Result(ResultStatus.OK, null))
    }

    @Test
    fun `should remove finalizer`() {
        manager.removeFinalizer()
        verify(controller, times(1)).removeFinalizer()
    }

    @Test
    fun `should change status on cluster terminated event`() {
        manager.onClusterTerminated()
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Terminated))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updated))
    }

    @Test
    fun `should change status on cluster stopped event`() {
        manager.onClusterStopped()
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Stopped))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on cluster started event`() {
        manager.onClusterStarted()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Started))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on cluster unhealthy event`() {
        manager.onClusterUnhealthy()
        verify(controller, times(1)).setShouldRestart(eq(true))
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Stopping))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on cluster ready to restart event`() {
        manager.onClusterReadyToRestart()
        verify(controller, times(1)).updateStatus()
        verify(controller, times(1)).updateDigests()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Starting))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on resource initialized event`() {
        manager.onResourceInitialise()
        verify(controller, times(1)).initializeAnnotations()
        verify(controller, times(1)).initializeStatus()
        verify(controller, times(1)).updateDigests()
        verify(controller, times(1)).addFinalizer()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Starting))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on resource diverged event`() {
        manager.onResourceDiverged()
        verify(controller, times(1)).setShouldRestart(eq(true))
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Stopping))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on resource changed event`() {
        manager.onResourceChanged()
        verify(controller, times(1)).setShouldRestart(eq(true))
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Stopping))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
    }

    @Test
    fun `should change status on resource deleted event`() {
        manager.onResourceDeleted()
        verify(controller, times(1)).setDeleteResources(true)
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).resetAction()
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Stopping))
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
    fun `stopCluster should return false when jobmanager pod is present`() {
        given(controller.doesJobManagerPodExists()).thenReturn(true)
        given(controller.doesTaskManagerPodsExist()).thenReturn(false)
        val result = manager.stopCluster()
        verify(controller, times(1)).doesJobManagerPodExists()
        verify(controller, times(1)).doesTaskManagerPodsExist()
        verify(controller, times(1)).getJobManagerReplicas()
        verify(controller, times(1)).deletePods(eq(DeleteOptions(label = "role", value = "jobmanager", limit = 1)))
        assertThat(result).isFalse()
    }

    @Test
    fun `stopCluster should return false when taskmanager pod is present`() {
        given(controller.doesJobManagerPodExists()).thenReturn(false)
        given(controller.doesTaskManagerPodsExist()).thenReturn(true)
        val result = manager.stopCluster()
        verify(controller, times(1)).doesJobManagerPodExists()
        verify(controller, times(1)).doesTaskManagerPodsExist()
        verify(controller, times(1)).getTaskManagerReplicas()
        verify(controller, times(1)).deletePods(eq(DeleteOptions(label = "role", value = "taskmanager", limit = 2)))
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return false when jobmanager service is present`() {
        given(controller.doesJobManagerPodExists()).thenReturn(false)
        given(controller.doesTaskManagerPodsExist()).thenReturn(false)
        given(controller.doesJobManagerServiceExists()).thenReturn(true)
        val result = manager.stopCluster()
        verify(controller, times(1)).doesJobManagerPodExists()
        verify(controller, times(1)).doesTaskManagerPodsExist()
        verify(controller, times(1)).doesJobManagerServiceExists()
        verify(controller, times(1)).deleteService()
        assertThat(result).isFalse()
    }

    @Test
    fun `startCluster should return true when jobmanager, taskmanager, and service are not present`() {
        given(controller.doesJobManagerPodExists()).thenReturn(false)
        given(controller.doesTaskManagerPodsExist()).thenReturn(false)
        given(controller.doesJobManagerServiceExists()).thenReturn(false)
        val result = manager.stopCluster()
        verify(controller, times(1)).doesJobManagerPodExists()
        verify(controller, times(1)).doesTaskManagerPodsExist()
        verify(controller, times(1)).doesJobManagerServiceExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when jobmanager pod is not present`() {
        given(controller.doesJobManagerPodExists()).thenReturn(false)
        val result = manager.hasResourceDiverged()
        verify(controller, times(1)).doesJobManagerPodExists()
        verify(controller, times(1)).doesJobManagerServiceExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when jobmanager service is not present`() {
        given(controller.doesJobManagerServiceExists()).thenReturn(false)
        val result = manager.hasResourceDiverged()
        verify(controller, times(1)).doesJobManagerPodExists()
        verify(controller, times(1)).doesJobManagerServiceExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return true when jobmanager service and jobmanager pod are present but there is less than one replica`() {
        given(controller.getJobManagerReplicas()).thenReturn(0)
        val result = manager.hasResourceDiverged()
        verify(controller, times(1)).doesJobManagerPodExists()
        verify(controller, times(1)).doesJobManagerServiceExists()
        verify(controller, times(1)).getJobManagerReplicas()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasResourceDiverged should return false when jobmanager service and jobmanager pod are present and there is one replica`() {
        given(controller.getJobManagerReplicas()).thenReturn(1)
        val result = manager.hasResourceDiverged()
        verify(controller, times(1)).doesJobManagerPodExists()
        verify(controller, times(1)).doesJobManagerServiceExists()
        verify(controller, times(1)).getJobManagerReplicas()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasSpecificationChanged should return true when resource has changed`() {
        given(controller.computeChanges()).thenReturn(listOf("TEST"))
        val result = manager.hasSpecificationChanged()
        verify(controller, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `hasSpecificationChanged should return false when resource hasn't changed`() {
        given(controller.computeChanges()).thenReturn(listOf())
        val result = manager.hasSpecificationChanged()
        verify(controller, times(1)).computeChanges()
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
    fun `hasScaleChanged should return false when scale hasn't changed`() {
        given(controller.getRequiredTaskManagers()).thenReturn(2)
        given(controller.getCurrentTaskManagers()).thenReturn(2)
        val result = manager.hasScaleChanged()
        verify(controller, times(1)).getRequiredTaskManagers()
        verify(controller, times(1)).getCurrentTaskManagers()
        assertThat(result).isFalse()
    }

    @Test
    fun `hasScaleChanged should return true when scale has changed`() {
        given(controller.getRequiredTaskManagers()).thenReturn(2)
        given(controller.getCurrentTaskManagers()).thenReturn(1)
        val result = manager.hasScaleChanged()
        verify(controller, times(1)).getRequiredTaskManagers()
        verify(controller, times(1)).getCurrentTaskManagers()
        assertThat(result).isTrue()
    }

    @Test
    fun `isManualActionPresent should return true when manual action is not none`() {
        given(controller.getAction()).thenReturn(ManualAction.START)
        val result = manager.isActionPresent()
        verify(controller, times(1)).getAction()
        assertThat(result).isTrue()
    }

    @Test
    fun `isManualActionPresent should return false when manual action is none`() {
        given(controller.getAction()).thenReturn(ManualAction.NONE)
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
    fun `mustRecreateResources should return true when jobmanager spec changed`() {
        given(controller.computeChanges()).thenReturn(listOf("JOB_MANAGER"))
        val result = manager.mustRecreateResources()
        verify(controller, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustRecreateResources should return true when taskmanager spec changed`() {
        given(controller.computeChanges()).thenReturn(listOf("TASK_MANAGER"))
        val result = manager.mustRecreateResources()
        verify(controller, times(1)).computeChanges()
        assertThat(result).isTrue()
    }

    @Test
    fun `mustRecreateResources should return true when runtime spec changed`() {
        given(controller.computeChanges()).thenReturn(listOf("RUNTIME"))
        val result = manager.mustRecreateResources()
        verify(controller, times(1)).computeChanges()
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
    fun `isClusterReady should return true when cluster is ready`() {
        given(controller.isClusterReady()).thenReturn(Result(ResultStatus.OK, true))
        val result = manager.isClusterReady()
        verify(controller, times(1)).isClusterReady()
        assertThat(result).isTrue()
    }

    @Test
    fun `isClusterReady should return false when cluster is not ready`() {
        given(controller.isClusterReady()).thenReturn(Result(ResultStatus.OK, false))
        val result = manager.isClusterReady()
        verify(controller, times(1)).isClusterReady()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterReady should return false when there is an error`() {
        given(controller.isClusterReady()).thenReturn(Result(ResultStatus.ERROR, true))
        val result = manager.isClusterReady()
        verify(controller, times(1)).isClusterReady()
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
    fun `isClusterHealthy should return true when cluster is healthy`() {
        given(controller.isClusterHealthy()).thenReturn(Result(ResultStatus.OK, true))
        val result = manager.isClusterHealthy()
        verify(controller, times(1)).isClusterHealthy()
        assertThat(result).isTrue()
    }

    @Test
    fun `isClusterHealthy should return false when cluster is not healthy`() {
        given(controller.isClusterHealthy()).thenReturn(Result(ResultStatus.OK, false))
        val result = manager.isClusterHealthy()
        verify(controller, times(1)).isClusterHealthy()
        assertThat(result).isFalse()
    }

    @Test
    fun `isClusterHealthy should return false when there is an error`() {
        given(controller.isClusterHealthy()).thenReturn(Result(ResultStatus.ERROR, true))
        val result = manager.isClusterHealthy()
        verify(controller, times(1)).isClusterHealthy()
        assertThat(result).isFalse()
    }

    @Test
    fun `areJobsUpdating should return true when there are some jobs in updating status`() {
        given(controller.areJobsUpdating()).thenReturn(true)
        val result = manager.areJobsUpdating()
        verify(controller, times(1)).areJobsUpdating()
        assertThat(result).isTrue()
    }

    @Test
    fun `areJobsUpdating should return false when there isn't any job in updating status`() {
        given(controller.areJobsUpdating()).thenReturn(false)
        val result = manager.areJobsUpdating()
        verify(controller, times(1)).areJobsUpdating()
        assertThat(result).isFalse()
    }

    @Test
    fun `ensureJobManagerServiceExists should do nothing when jobmanager service exists`() {
        given(controller.doesJobManagerServiceExists()).thenReturn(true)
        val result = manager.ensureJobManagerServiceExists()
        verify(controller, times(1)).doesJobManagerServiceExists()
        assertThat(result).isTrue()
    }

    @Test
    fun `ensureJobManagerServiceExists should create resource when jobmanager service doesn't exists`() {
        given(controller.doesJobManagerServiceExists()).thenReturn(false)
        given(controller.createService()).thenReturn(Result(ResultStatus.OK, "test"))
        val result = manager.ensureJobManagerServiceExists()
        verify(controller, times(1)).doesJobManagerServiceExists()
        verify(controller, times(1)).createService()
        assertThat(result).isFalse()
    }

    @Test
    fun `ensureJobManagerPodExists should do nothing when jobmanager pod exists`() {
        given(controller.getJobManagerReplicas()).thenReturn(1)
        val result = manager.ensureJobManagerPodExists()
        verify(controller, times(1)).getJobManagerReplicas()
        assertThat(result).isTrue()
    }

    @Test
    fun `ensureJobManagerPodExists should create resource when jobmanager pod doesn't exists`() {
        given(controller.getJobManagerReplicas()).thenReturn(0)
        given(controller.createJobManagerPods(eq(1))).thenReturn(Result(ResultStatus.OK, setOf()))
        val result = manager.ensureJobManagerPodExists()
        verify(controller, times(1)).getJobManagerReplicas()
        verify(controller, times(1)).createJobManagerPods(1)
        assertThat(result).isFalse()
    }

    @Test
    fun `rescaleTaskManagers should do nothing when required taskmanagers are equals to current taskmanagers`() {
        given(controller.getTaskManagers()).thenReturn(2)
        given(controller.getRequiredTaskManagers()).thenReturn(2)
        val result = manager.rescaleTaskManagers()
        verify(controller, times(1)).getTaskManagers()
        verify(controller, times(1)).getRequiredTaskManagers()
        assertThat(result).isFalse()
    }

    @Test
    fun `rescaleTaskManagers should rescale cluster when required taskmanagers are not equals to current taskmanagers`() {
        given(controller.getTaskManagers()).thenReturn(3)
        given(controller.getRequiredTaskManagers()).thenReturn(2)
        val result = manager.rescaleTaskManagers()
        verify(controller, times(1)).getTaskManagers()
        verify(controller, times(1)).getRequiredTaskManagers()
        verify(controller, times(1)).rescaleCluster(eq(2))
        assertThat(result).isTrue()
    }

    @Test
    fun `rescaleTaskManagerPods should do nothing when taskmanager replicas are equals to current taskmanagers`() {
        given(controller.getTaskManagers()).thenReturn(2)
        given(controller.getTaskManagerReplicas()).thenReturn(2)
        val result = manager.rescaleTaskManagerPods()
        verify(controller, times(1)).getTaskManagers()
        verify(controller, times(1)).getTaskManagerReplicas()
        assertThat(result).isFalse()
    }

    @Test
    fun `rescaleTaskManagerPods should do nothing when taskmanager replicas are more than current taskmanagers but timeout didn't occur`() {
        given(controller.getTaskManagers()).thenReturn(2)
        given(controller.getTaskManagerReplicas()).thenReturn(3)
        given(controller.getRescaleDelay()).thenReturn(60)
        given(controller.timeSinceLastRescaleInSeconds()).thenReturn(50)
        val result = manager.rescaleTaskManagerPods()
        verify(controller, times(1)).getTaskManagers()
        verify(controller, times(2)).getTaskManagerReplicas()
        assertThat(result).isTrue()
    }

    @Test
    fun `rescaleTaskManagerPods should create pods when taskmanager replicas are less than current taskmanagers`() {
        given(controller.getTaskManagers()).thenReturn(3)
        given(controller.getTaskManagerReplicas()).thenReturn(2)
        given(controller.createTaskManagerPods(eq(3))).thenReturn(Result(ResultStatus.OK, setOf("test")))
        val result = manager.rescaleTaskManagerPods()
        verify(controller, times(1)).getTaskManagers()
        verify(controller, times(2)).getTaskManagerReplicas()
        verify(controller, times(1)).createTaskManagerPods(eq(3))
        assertThat(result).isTrue()
    }

    @Test
    fun `rescaleTaskManagerPods should delete pods when taskmanager replicas are more than current taskmanagers and timeout occurred`() {
        given(controller.getTaskManagers()).thenReturn(2)
        given(controller.getTaskManagerReplicas()).thenReturn(3)
        given(controller.getRescaleDelay()).thenReturn(60)
        given(controller.timeSinceLastRescaleInSeconds()).thenReturn(70)
        val result = manager.rescaleTaskManagerPods()
        verify(controller, times(1)).getTaskManagers()
        verify(controller, times(2)).getTaskManagerReplicas()
        verify(controller, times(1)).removeUnusedTaskManagers()
        assertThat(result).isTrue()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is none`() {
        given(controller.getAction()).thenReturn(ManualAction.NONE)
        manager.executeAction(setOf(ManualAction.NONE))
        verify(controller, times(1)).getAction()
        verifyNoMoreInteractions(controller)
    }

    @Test
    fun `executeManualAction should do nothing when manual action is start but action is not allowed`() {
        given(controller.getAction()).thenReturn(ManualAction.START)
        manager.executeAction(setOf())
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should do nothing when manual action is stop but action is not allowed`() {
        given(controller.getAction()).thenReturn(ManualAction.STOP)
        manager.executeAction(setOf())
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is start`() {
        given(controller.getAction()).thenReturn(ManualAction.START)
        manager.executeAction(setOf(ManualAction.START))
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).updateStatus()
        verify(controller, times(1)).updateDigests()
        verify(controller, times(1)).setShouldRestart(eq(true))
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Starting))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `executeManualAction should change status when manual action is stop`() {
        given(controller.getAction()).thenReturn(ManualAction.STOP)
        manager.executeAction(setOf(ManualAction.STOP))
        verify(controller, times(1)).getAction()
        verify(controller, times(1)).setShouldRestart(eq(false))
        verify(controller, times(1)).setSupervisorStatus(eq(ClusterStatus.Stopping))
        verify(controller, times(1)).setResourceStatus(eq(ResourceStatus.Updating))
        verify(controller, times(1)).resetAction()
    }

    @Test
    fun `stopAllJobs should return true when all jobs have been stopped`() {
        given(controller.stopJobs(eq(setOf()))).thenReturn(Result(ResultStatus.OK, true))
        val result = manager.stopAllJobs()
        verify(controller, times(1)).stopJobs(eq(setOf()))
        assertThat(result).isTrue()
    }

    @Test
    fun `stopAllJobs should return false when not all jobs have been stopped`() {
        given(controller.stopJobs(eq(setOf()))).thenReturn(Result(ResultStatus.ERROR, true))
        val result = manager.stopAllJobs()
        verify(controller, times(1)).stopJobs(eq(setOf()))
        assertThat(result).isFalse()
    }

    @Test
    fun `stopUnmanagedJobs should return true when there jobs are not defined`() {
        given(controller.getJobSpecs()).thenReturn(listOf())
        val result = manager.stopUnmanagedJobs()
        verify(controller, times(1)).getJobSpecs()
        assertThat(result).isTrue()
    }

    @Test
    fun `stopUnmanagedJobs should return true when jobs have been stopped`() {
        given(controller.getJobSpecs()).thenReturn(listOf(V2FlinkClusterJobSpec()))
        given(controller.getJobIds()).thenReturn(setOf("123"))
        given(controller.stopJobs(eq(setOf("123")))).thenReturn(Result(ResultStatus.OK, true))
        val result = manager.stopUnmanagedJobs()
        verify(controller, times(1)).getJobSpecs()
        verify(controller, times(1)).getJobIds()
        assertThat(result).isTrue()
    }

    @Test
    fun `stopUnmanagedJobs should return false when jobs haven't been stopped`() {
        given(controller.getJobSpecs()).thenReturn(listOf(V2FlinkClusterJobSpec()))
        given(controller.getJobIds()).thenReturn(setOf("123"))
        given(controller.stopJobs(eq(setOf("123")))).thenReturn(Result(ResultStatus.ERROR, true))
        val result = manager.stopUnmanagedJobs()
        verify(controller, times(1)).getJobSpecs()
        verify(controller, times(1)).getJobIds()
        assertThat(result).isFalse()
    }

    @Test
    fun `createJobs should return true when jobs have been created`() {
        given(controller.haveJobsBeenCreated()).thenReturn(true)
        val result = manager.createJobs()
        verify(controller, times(1)).haveJobsBeenCreated()
        assertThat(result).isTrue()
    }

    @Test
    fun `createJobs should return false when jobs haven't been created`() {
        val jobSpec = V2FlinkClusterJobSpec()
        jobSpec.name = "test"
        given(controller.getJobSpecs()).thenReturn(listOf(jobSpec))
        given(controller.haveJobsBeenCreated()).thenReturn(false)
        given(controller.listExistingJobNames()).thenReturn(listOf())
        given(controller.createJob(any<V2FlinkClusterJobSpec>())).thenReturn((Result(ResultStatus.OK, null)))
        val result = manager.createJobs()
        verify(controller, times(1)).haveJobsBeenCreated()
        verify(controller, times(1)).getJobSpecs()
        verify(controller, times(1)).createJob(any<V2FlinkClusterJobSpec>())
        assertThat(result).isFalse()
    }

    @Test
    fun `waitForJobs should return false when jobs haven't been stopped`() {
        given(controller.getJobNamesWithStatus()).thenReturn(mapOf("test" to ClusterStatus.Started.toString()))
        val result = manager.waitForJobs()
        verify(controller, times(1)).getJobNamesWithStatus()
        assertThat(result).isFalse()
    }

    @Test
    fun `waitForJobs should return true when jobs have been stopped`() {
        given(controller.getJobNamesWithStatus()).thenReturn(mapOf("test" to ClusterStatus.Stopped.toString()))
        val result = manager.waitForJobs()
        verify(controller, times(1)).getJobNamesWithStatus()
        assertThat(result).isTrue()
    }

    @Test
    fun `deleteJobs should return true when jobs have been deleted`() {
        given(controller.haveJobsBeenRemoved()).thenReturn(true)
        val result = manager.deleteJobs()
        verify(controller, times(1)).haveJobsBeenRemoved()
        assertThat(result).isTrue()
    }

    @Test
    fun `deleteJobs should return false when jobs haven't been deleted`() {
        val jobSpec = V2FlinkClusterJobSpec()
        jobSpec.name = "test"
        given(controller.getJobSpecs()).thenReturn(listOf(jobSpec))
        given(controller.haveJobsBeenRemoved()).thenReturn(false)
        given(controller.listExistingJobNames()).thenReturn(listOf("test"))
        given(controller.deleteJob("test")).thenReturn(Result(ResultStatus.OK, null))
        val result = manager.deleteJobs()
        verify(controller, times(1)).haveJobsBeenRemoved()
        verify(controller, times(1)).listExistingJobNames()
        verify(controller, times(1)).getJobSpecs()
        verify(controller, times(1)).deleteJob("test")
        assertThat(result).isFalse()
    }

    @Test
    fun `deleteJobs should not delete job when jobs haven't been deleted and job is not defined`() {
        val jobSpec = V2FlinkClusterJobSpec()
        jobSpec.name = "other"
        given(controller.getJobSpecs()).thenReturn(listOf(jobSpec))
        given(controller.haveJobsBeenRemoved()).thenReturn(false)
        given(controller.listExistingJobNames()).thenReturn(listOf("test"))
        val result = manager.deleteJobs()
        verify(controller, times(1)).haveJobsBeenRemoved()
        verify(controller, times(1)).listExistingJobNames()
        verify(controller, times(1)).getJobSpecs()
        assertThat(result).isFalse()
    }
}
