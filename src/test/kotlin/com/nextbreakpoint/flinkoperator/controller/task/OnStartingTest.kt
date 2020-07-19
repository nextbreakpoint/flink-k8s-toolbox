package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
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
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.atLeast
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class OnStartingTest {
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val clusterScaling = ClusterScaling(taskManagers = 2, taskSlots = 1)
    private val logger = mock(Logger::class.java)
    private val context = mock(TaskContext::class.java)
    private val task = OnStarting(logger)

    @BeforeEach
    fun configure() {
        given(context.clusterSelector).thenReturn(clusterSelector)
        given(context.hasBeenDeleted()).thenReturn(false)
        given(context.getClusterScale()).thenReturn(clusterScaling)
        given(context.getJobManagerReplicas()).thenReturn(1)
        given(context.getTaskManagerReplicas()).thenReturn(2)
        given(context.isBootstrapPresent()).thenReturn(true)
        given(context.isClusterReady(any(), any())).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        given(context.removeJar(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        given(context.isJobRunning(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(10)
        given(context.doesBootstrapJobExists()).thenReturn(true)
        given(context.doesJobManagerServiceExists()).thenReturn(true)
        given(context.doesJobManagerStatefulSetExists()).thenReturn(true)
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(true)
        given(context.createBootstrapJob(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = "test-bootstrap-job"))
        given(context.createJobManagerService(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = "test-jobmanager-service"))
        given(context.createJobManagerStatefulSet(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = "test-jobmanager-statefulset"))
        given(context.createTaskManagerStatefulSet(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = "test-taskmanager-statefulset"))
    }

    @Test
    fun `should change status to running when cluster has all resources and job is running`() {
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getClusterScale()
        verify(context, times(1)).getJobManagerReplicas()
        verify(context, times(1)).getTaskManagerReplicas()
        verify(context, times(1)).isBootstrapPresent()
        verify(context, times(1)).isJobRunning(any())
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(eq(ClusterStatus.Running))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to running when cluster has all resources and bootstrap job is not defined`() {
        given(context.isBootstrapPresent()).thenReturn(false)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getClusterScale()
        verify(context, times(1)).getJobManagerReplicas()
        verify(context, times(1)).getTaskManagerReplicas()
        verify(context, times(1)).isBootstrapPresent()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(eq(ClusterStatus.Running))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should do nothing when cluster has all resources but job is not running`() {
        given(context.isJobRunning(any())).thenReturn(OperationResult(status = OperationStatus.OK, output = false))
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getClusterScale()
        verify(context, times(1)).getJobManagerReplicas()
        verify(context, times(1)).getTaskManagerReplicas()
        verify(context, times(1)).isBootstrapPresent()
        verify(context, times(1)).isJobRunning(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should create bootstrap job when bootstrap job does not exist and cluster is ready`() {
        given(context.doesBootstrapJobExists()).thenReturn(false)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getClusterScale()
        verify(context, times(1)).getJobManagerReplicas()
        verify(context, times(1)).getTaskManagerReplicas()
        verify(context, times(1)).isBootstrapPresent()
        verify(context, times(1)).isClusterReady(eq(clusterSelector), eq(clusterScaling))
        verify(context, times(1)).removeJar(eq(clusterSelector))
        verify(context, times(1)).createBootstrapJob(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should do nothing when bootstrap job does not exist and cluster is not ready`() {
        given(context.isClusterReady(any(), any())).thenReturn(OperationResult(status = OperationStatus.OK, output = false))
        given(context.doesBootstrapJobExists()).thenReturn(false)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getClusterScale()
        verify(context, times(1)).getJobManagerReplicas()
        verify(context, times(1)).getTaskManagerReplicas()
        verify(context, times(1)).isBootstrapPresent()
        verify(context, times(1)).isClusterReady(eq(clusterSelector), eq(clusterScaling))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should do nothing when bootstrap job does not exist and jar has not been removed`() {
        given(context.removeJar(any())).thenReturn(OperationResult(status = OperationStatus.ERROR, output = null))
        given(context.doesBootstrapJobExists()).thenReturn(false)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getClusterScale()
        verify(context, times(1)).getJobManagerReplicas()
        verify(context, times(1)).getTaskManagerReplicas()
        verify(context, times(1)).isBootstrapPresent()
        verify(context, times(1)).isClusterReady(eq(clusterSelector), eq(clusterScaling))
        verify(context, times(1)).removeJar(eq(clusterSelector))
        verify(context, times(1)).doesBootstrapJobExists()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should restart pods when jobmanager replicas is not equal to one`() {
        given(context.getJobManagerReplicas()).thenReturn(0)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getClusterScale()
        verify(context, times(1)).getJobManagerReplicas()
        verify(context, times(1)).getTaskManagerReplicas()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).restartPods(eq(clusterSelector), eq(clusterScaling))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should restart pods when taskmanager replicas is not equal to required taskmanagers`() {
        given(context.getTaskManagerReplicas()).thenReturn(1)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).getClusterScale()
        verify(context, times(1)).getJobManagerReplicas()
        verify(context, times(1)).getTaskManagerReplicas()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).restartPods(eq(clusterSelector), eq(clusterScaling))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should create jobmanager service when service does not exist`() {
        given(context.doesJobManagerServiceExists()).thenReturn(false)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).createJobManagerService(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should create jobmanager statefulset when statefulset does not exist`() {
        given(context.doesJobManagerStatefulSetExists()).thenReturn(false)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).createJobManagerStatefulSet(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should create taskmanager statefulset when statefulset does not exist`() {
        given(context.doesTaskManagerStatefulSetExists()).thenReturn(false)
        task.execute(context)
        verify(logger, atLeast(1)).info(any())
        verifyNoMoreInteractions(logger)
        verify(context, atLeast(1)).clusterSelector
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).doesJobManagerServiceExists()
        verify(context, times(1)).doesJobManagerStatefulSetExists()
        verify(context, times(1)).doesTaskManagerStatefulSetExists()
        verify(context, times(1)).createTaskManagerStatefulSet(eq(clusterSelector))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to failed if cluster is not running after timeout`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(301)
        task.execute(context)
        verify(logger, times(1)).error(any())
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).resetSavepointRequest()
        verify(context, times(1)).setClusterStatus(eq(ClusterStatus.Failed))
        verifyNoMoreInteractions(context)
    }

    @Test
    fun `should change status to cancelling if cluster has been deleted`() {
        given(context.hasBeenDeleted()).thenReturn(true)
        task.execute(context)
        verifyNoMoreInteractions(logger)
        verify(context, times(1)).hasBeenDeleted()
        verify(context, times(1)).resetManualAction()
        verify(context, times(1)).setDeleteResources(ArgumentMatchers.eq(true))
        verify(context, times(1)).setClusterStatus(eq(ClusterStatus.Cancelling))
        verifyNoMoreInteractions(context)
    }
}