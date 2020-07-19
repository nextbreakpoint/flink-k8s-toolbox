package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import org.apache.log4j.Logger
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.atLeast
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class OnStoppingTest {
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val logger = mock(Logger::class.java)
    private val context = mock(TaskContext::class.java)
    private val task = OnStopping(logger)

    @BeforeEach
    fun configure() {
        given(context.clusterSelector).thenReturn(clusterSelector)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(10)
        given(context.isDeleteResources()).thenReturn(false)
        given(context.arePodsTerminated(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        given(context.doesBootstrapJobExists()).thenReturn(false)
        given(context.doesJobManagerServiceExists()).thenReturn(false)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(false)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(false)
        given(context.doesJobManagerPVCExists()).thenReturn(false)
        given(context.doesTaskManagerPVCExists()).thenReturn(false)
        given(context.deleteJobManagerService(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        given(context.deleteBootstrapJob(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        given(context.deleteStatefulSets(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        given(context.deletePersistentVolumeClaims(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
    }

    @Test
    fun `should change status to suspended when cluster resources have been stopped`() {
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isDeleteResources()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(eq(ClusterStatus.Suspended))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to terminated when cluster resources have been deleted`() {
        given(context.isDeleteResources()).thenReturn(true)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isDeleteResources()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(eq(ClusterStatus.Terminated))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to failed when cluster is not stopped after timeout`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(301)
        task.execute(context)
        verify(logger, times(1)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(eq(ClusterStatus.Failed))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete jobmanager service if service exists`() {
        given(context.doesJobManagerServiceExists()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isDeleteResources()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).deleteJobManagerService(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete bootstrap job if job exists`() {
        given(context.doesBootstrapJobExists()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isDeleteResources()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).deleteBootstrapJob(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should terminate jobmanager and taskmanager pods if there are pods running`() {
        given(context.arePodsTerminated(eq(clusterSelector))).thenReturn(OperationResult(status = OperationStatus.OK, output = false))
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isDeleteResources()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).terminatePods(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete jobmanager and taskmanager PVCs if PVCs exist and resources must be deleted`() {
        given(context.isDeleteResources()).thenReturn(true)
        given(context.doesJobManagerPVCExists()).thenReturn(true)
        given(context.doesTaskManagerPVCExists()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isDeleteResources()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).deletePersistentVolumeClaims(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete jobmanager and taskmanager statefulsets if statefulsets exist and resources must be deleted`() {
        given(context.isDeleteResources()).thenReturn(true)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(true)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isDeleteResources()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).deleteStatefulSets(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete jobmanager service if service exist and resources must be deleted`() {
        given(context.isDeleteResources()).thenReturn(true)
        given(context.doesJobManagerServiceExists()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isDeleteResources()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).deleteJobManagerService(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should delete bootstrap job if job exist and resources must be deleted`() {
        given(context.isDeleteResources()).thenReturn(true)
        given(context.doesBootstrapJobExists()).thenReturn(true)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isDeleteResources()
        verify(context, times(1)).arePodsTerminated(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).doesJobManagerPVCExists()
        verify(context, times(1)).doesTaskManagerPVCExists()
        verify(context, times(1)).deleteBootstrapJob(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }
}