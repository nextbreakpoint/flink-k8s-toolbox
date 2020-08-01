package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterScale
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.DeleteOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.PodReplicas
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.apache.log4j.Logger
import org.assertj.core.api.Assertions.assertThat
import org.joda.time.DateTime
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify

class TaskMediatorTest {
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val clusterSelector = ClusterSelector(name = "test", namespace = "flink", uuid = "123")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val savepointOptions = SavepointOptions(targetPath = "file:///tmp")
    private val podReplicas = PodReplicas(pod = TestFactory.aTaskManagerPod(cluster,"1"), replicas = 2)
    private val clusterScale = ClusterScale(taskManagers = 2, taskSlots = 2)
    private val deleteOptions = DeleteOptions(label = "role", value = "jobmanager", limit = 1)
    private val resources = CachedResources(
        flinkCluster = cluster,
        bootstrapJob = TestFactory.aBootstrapJob(cluster),
        service = TestFactory.aJobManagerService(cluster),
        jobmanagerPods = setOf(TestFactory.aJobManagerPod(cluster,"1")),
        taskmanagerPods = setOf(TestFactory.aTaskManagerPod(cluster,"1"))
    )
    private val logger = mock(Logger::class.java)
    private val controller = mock(OperationController::class.java)
    private val context = TaskMediator(clusterSelector, cluster, resources, controller)

    @Test
    fun `should update savepoint request`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        val savepointRequest = savepointRequest
        assertThat(Status.getSavepointRequest(cluster)).isNull()
        context.setSavepointRequest(savepointRequest)
        assertThat(Status.getSavepointRequest(cluster)).isEqualTo(savepointRequest)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return savepoint request`() {
        val savepointRequest = savepointRequest
        assertThat(context.getSavepointRequest()).isNull()
        Status.setSavepointRequest(cluster, savepointRequest)
        assertThat(Status.getSavepointRequest(cluster)).isEqualTo(savepointRequest)
    }

    @Test
    fun `should reset savepoint request`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        val savepointRequest = savepointRequest
        Status.setSavepointRequest(cluster, savepointRequest)
        context.resetSavepointRequest()
        assertThat(Status.getSavepointRequest(cluster)).isNull()
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update savepoint path`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getSavepointPath(cluster)).isNull()
        context.setSavepointPath("file:///tmp/1")
        assertThat(Status.getSavepointPath(cluster)).isEqualTo("file:///tmp/1")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update cluster status`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Unknown)
        context.setClusterStatus(ClusterStatus.Running)
        assertThat(context.getClusterStatus()).isEqualTo(ClusterStatus.Running)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return cluster status`() {
        assertThat(context.getClusterStatus()).isEqualTo(ClusterStatus.Unknown)
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        assertThat(context.getClusterStatus()).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update delete resources`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Annotations.getActionTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Annotations.isDeleteResources(cluster)).isEqualTo(false)
        context.setDeleteResources(true)
        assertThat(Annotations.isDeleteResources(cluster)).isEqualTo(true)
        assertThat(Annotations.getActionTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should reset manual action`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Annotations.getActionTimestamp(cluster)).isEqualTo(DateTime(0))
        Annotations.setManualAction(cluster, ManualAction.STOP)
        context.resetManualAction()
        assertThat(Annotations.getManualAction(cluster)).isEqualTo(ManualAction.NONE)
        assertThat(Annotations.getActionTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return savepoint options`() {
        cluster.spec?.operator?.savepointTargetPath = "file:///tmp"
        val savepointOptions = savepointOptions
        assertThat(context.getSavepointOptions()).isEqualTo(savepointOptions)
    }

    @Test
    fun `should return savepoint interval`() {
        assertThat(context.getSavepointInterval()).isEqualTo(60)
        cluster.spec?.operator?.savepointInterval = 200
        assertThat(context.getSavepointInterval()).isEqualTo(200)
    }

    @Test
    fun `should return savepoint mode`() {
        Status.setSavepointMode(cluster, "Manual")
        assertThat(context.getSavepointMode()).isEqualTo("Manual")
    }

    @Test
    fun `should return has been deleted`() {
        assertThat(context.hasBeenDeleted()).isEqualTo(false)
        cluster.metadata.deletionTimestamp = DateTime(System.currentTimeMillis())
        assertThat(context.hasBeenDeleted()).isEqualTo(true)
    }

    @Test
    fun `should return has finalizer`() {
        assertThat(context.hasFinalizer()).isEqualTo(false)
        cluster.metadata.finalizers = listOf("finalizer.nextbreakpoint.com")
        assertThat(context.hasFinalizer()).isEqualTo(true)
    }

    @Test
    fun `should add finalizer`() {
        assertThat(context.hasFinalizer()).isEqualTo(false)
        context.addFinalizer()
        assertThat(context.hasFinalizer()).isEqualTo(true)
    }

    @Test
    fun `should remove finalizer`() {
        cluster.metadata.finalizers = listOf("finalizer.nextbreakpoint.com")
        assertThat(context.hasFinalizer()).isEqualTo(true)
        context.removeFinalizer()
        assertThat(context.hasFinalizer()).isEqualTo(false)
    }

    @Test
    fun `should initialize status`() {
        assertThat(Status.getBootstrap(cluster)).isNull()
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(0)
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(0)
        assertThat(Status.getJobParallelism(cluster)).isEqualTo(0)
        assertThat(Status.getSavepointPath(cluster)).isNull()
        assertThat(Status.getLabelSelector(cluster)).isNull()
        assertThat(Status.getServiceMode(cluster)).isNull()
        assertThat(Status.getSavepointMode(cluster)).isNull()
        assertThat(Status.getRestartPolicy(cluster)).isNull()
        cluster.spec?.operator?.savepointPath = "file:///tmp/1"
        context.initializeStatus()
        assertThat(Status.getBootstrap(cluster)).isNotNull()
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(1)
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(1)
        assertThat(Status.getJobParallelism(cluster)).isEqualTo(1)
        assertThat(Status.getSavepointPath(cluster)).isEqualTo("file:///tmp/1")
        assertThat(Status.getLabelSelector(cluster)).isNotEmpty()
        assertThat(Status.getServiceMode(cluster)).isEqualTo("ClusterIP")
        assertThat(Status.getSavepointMode(cluster)).isEqualTo("Automatic")
        assertThat(Status.getRestartPolicy(cluster)).isEqualTo("Never")
    }

    @Test
    fun `should initialize annotations`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Annotations.getActionTimestamp(cluster)).isEqualTo(DateTime(0))
        Annotations.setManualAction(cluster, ManualAction.STOP)
        Annotations.setDeleteResources(cluster, true)
        Annotations.setWithoutSavepoint(cluster, true)
        context.initializeAnnotations()
        assertThat(Annotations.getManualAction(cluster)).isEqualTo(ManualAction.NONE)
        assertThat(Annotations.isDeleteResources(cluster)).isFalse()
        assertThat(Annotations.isWithoutSavepoint(cluster)).isFalse()
        assertThat(Annotations.getActionTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update digests`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getBootstrapDigest(cluster)).isNull()
        assertThat(Status.getRuntimeDigest(cluster)).isNull()
        assertThat(Status.getJobManagerDigest(cluster)).isNull()
        assertThat(Status.getTaskManagerDigest(cluster)).isNull()
        context.updateDigests()
        assertThat(Status.getBootstrapDigest(cluster)).isNotNull()
        assertThat(Status.getRuntimeDigest(cluster)).isNotNull()
        assertThat(Status.getJobManagerDigest(cluster)).isNotNull()
        assertThat(Status.getTaskManagerDigest(cluster)).isNotNull()
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update status`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getBootstrap(cluster)).isNull()
        assertThat(Status.getServiceMode(cluster)).isNull()
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(0)
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(0)
        cluster.spec?.taskManagers = 2
        cluster.spec?.taskManager?.taskSlots = 3
        context.updateStatus()
        assertThat(Status.getBootstrap(cluster)).isNotNull()
        assertThat(Status.getServiceMode(cluster)).isNotNull()
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(2)
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(3)
        assertThat(Status.getJobParallelism(cluster)).isEqualTo(6)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return no changes when digests didn't change`() {
        context.updateDigests()
        val changes = context.computeChanges()
        assertThat(changes).isEmpty()
    }

    @Test
    fun `should return one change when bootstrap digest changed`() {
        context.updateDigests()
        Status.setBootstrapDigest(cluster, "0")
        val changes = context.computeChanges()
        assertThat(changes).containsExactly("BOOTSTRAP")
    }

    @Test
    fun `should return one change when runtime digest changed`() {
        context.updateDigests()
        Status.setRuntimeDigest(cluster, "0")
        val changes = context.computeChanges()
        assertThat(changes).containsExactly("RUNTIME")
    }

    @Test
    fun `should return one change when jobmanager digest changed`() {
        context.updateDigests()
        Status.setJobManagerDigest(cluster, "0")
        val changes = context.computeChanges()
        assertThat(changes).containsExactly("JOB_MANAGER")
    }

    @Test
    fun `should return one change when taskmanager digest changed`() {
        context.updateDigests()
        Status.setTaskManagerDigest(cluster, "0")
        val changes = context.computeChanges()
        assertThat(changes).containsExactly("TASK_MANAGER")
    }

    @Test
    fun `should return bootstrap present`() {
        context.updateStatus()
        assertThat(context.isBootstrapPresent()).isTrue()
        Status.setBootstrap(cluster, null)
        assertThat(context.isBootstrapPresent()).isFalse()
    }

    @Test
    fun `should rescale cluster`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        Status.setTaskSlots(cluster, 2)
        cluster.spec?.taskManagers = 4
        context.rescaleCluster()
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(4)
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(2)
        assertThat(Status.getJobParallelism(cluster)).isEqualTo(8)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return number of task managers`() {
        assertThat(context.getTaskManagers()).isEqualTo(0)
        Status.setTaskManagers(cluster, 4)
        assertThat(context.getTaskManagers()).isEqualTo(4)
    }

    @Test
    fun `should return cluster scale`() {
        assertThat(context.getClusterScale()).isEqualTo(ClusterScale(taskSlots = 0, taskManagers = 0))
        Status.setTaskManagers(cluster, 4)
        Status.setTaskSlots(cluster, 2)
        assertThat(context.getClusterScale()).isEqualTo(ClusterScale(taskSlots = 2, taskManagers = 4))
    }

    @Test
    fun `should return action timestamp`() {
        assertThat(context.getActionTimestamp()).isEqualTo(Annotations.getActionTimestamp(cluster))
        Annotations.setManualAction(cluster, ManualAction.STOP)
        assertThat(context.getActionTimestamp()).isEqualTo(Annotations.getActionTimestamp(cluster))
    }

    @Test
    fun `should return status timestamp`() {
        assertThat(context.getStatusTimestamp()).isEqualTo(Status.getStatusTimestamp(cluster))
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        assertThat(context.getStatusTimestamp()).isEqualTo(Status.getStatusTimestamp(cluster))
    }

    @Test
    fun `should return manual action`() {
        assertThat(context.getManualAction()).isEqualTo(ManualAction.NONE)
        Annotations.setManualAction(cluster, ManualAction.STOP)
        assertThat(context.getManualAction()).isEqualTo(ManualAction.STOP)
    }

    @Test
    fun `should return job restart policy`() {
        assertThat(context.getRestartPolicy()).isNull()
        Status.setRestartPolicy(cluster, "Never")
        assertThat(context.getRestartPolicy()).isEqualTo("Never")
    }

    @Test
    fun `should return desired number of task managers`() {
        assertThat(context.getDesiredTaskManagers()).isEqualTo(1)
        cluster.spec?.taskManagers = 4
        assertThat(context.getDesiredTaskManagers()).isEqualTo(4)
    }

    @Test
    fun `should return number of jobmanager replicas`() {
        val resources = resources.withJobManagerPods(setOf(
            TestFactory.aJobManagerPod(cluster,"1"),
            TestFactory.aJobManagerPod(cluster,"2")
        ))
        val context = TaskMediator(clusterSelector, cluster, resources, controller)
        assertThat(context.getJobManagerReplicas()).isEqualTo(2)
    }

    @Test
    fun `should return number of taskmanager replicas`() {
        val resources = resources.withTaskManagerPods(setOf(
            TestFactory.aTaskManagerPod(cluster,"1"),
            TestFactory.aTaskManagerPod(cluster,"2"),
            TestFactory.aTaskManagerPod(cluster,"3"),
            TestFactory.aTaskManagerPod(cluster,"4")
        ))
        val context = TaskMediator(clusterSelector, cluster, resources, controller)
        assertThat(context.getTaskManagerReplicas()).isEqualTo(4)
    }

    @Test
    fun `should return true when resource deleted otherwise false`() {
        Annotations.setDeleteResources(cluster, false)
        assertThat(context.isDeleteResources()).isFalse()
        Annotations.setDeleteResources(cluster, true)
        assertThat(context.isDeleteResources()).isTrue()
    }

    @Test
    fun `should return true when bootstrap exists otherwise false`() {
        assertThat(context.doesBootstrapJobExists()).isTrue()
        val newResource = resources.withBootstrapJob(null)
        val newMediator = TaskMediator(clusterSelector, cluster, newResource, controller)
        assertThat(newMediator.doesBootstrapJobExists()).isFalse()
    }

    @Test
    fun `should return true when jobmanager service exists otherwise false`() {
        assertThat(context.doesServiceExists()).isTrue()
        val newResource = resources.withService(null)
        val newMediator = TaskMediator(clusterSelector, cluster, newResource, controller)
        assertThat(newMediator.doesServiceExists()).isFalse()
    }

    @Test
    fun `should return true when jobmanager pod exists otherwise false`() {
        assertThat(context.doesJobManagerPodsExists()).isTrue()
        val newResource = resources.withJobManagerPods(setOf())
        val newMediator = TaskMediator(clusterSelector, cluster, newResource, controller)
        assertThat(newMediator.doesJobManagerPodsExists()).isFalse()
    }

    @Test
    fun `should return true when taskmanager pod exists otherwise false`() {
        assertThat(context.doesTaskManagerPodsExists()).isTrue()
        val newResource = resources.withTaskManagerPods(setOf())
        val newMediator = TaskMediator(clusterSelector, cluster, newResource, controller)
        assertThat(newMediator.doesTaskManagerPodsExists()).isFalse()
    }

    @Test
    fun `should return true when savepoint is required otherwise false`() {
        Annotations.setWithoutSavepoint(cluster, false)
        Annotations.setDeleteResources(cluster, false)
        assertThat(context.isSavepointRequired()).isEqualTo(true)
        Annotations.setWithoutSavepoint(cluster, true)
        Annotations.setDeleteResources(cluster, false)
        assertThat(context.isSavepointRequired()).isEqualTo(false)
        Annotations.setWithoutSavepoint(cluster, false)
        Annotations.setDeleteResources(cluster, true)
        assertThat(context.isSavepointRequired()).isEqualTo(false)
    }

    @Test
    fun `should refresh status`() {
        Status.setActiveTaskManagers(cluster, 0)
        Status.setTotalTaskSlots(cluster, 0)
        Status.setTaskSlots(cluster, 4)
        Status.setTaskManagers(cluster, 2)
        Status.setSavepointMode(cluster, "Manual")
        Status.setRestartPolicy(cluster, "Always")

        val resources = resources.withTaskManagerPods(setOf(
            TestFactory.aTaskManagerPod(cluster,"1"),
            TestFactory.aTaskManagerPod(cluster,"2"),
            TestFactory.aTaskManagerPod(cluster,"3"),
            TestFactory.aTaskManagerPod(cluster,"4")
        ))
        val context = TaskMediator(clusterSelector, cluster, resources, controller)

        cluster.metadata.finalizers = listOf("finalizer.nextbreakpoint.com")

        val timestamp = System.currentTimeMillis()

        context.refreshStatus(logger, DateTime(timestamp), DateTime(timestamp), false)

        assertThat(Status.getActiveTaskManagers(cluster)).isEqualTo(4)
        assertThat(Status.getTotalTaskSlots(cluster)).isEqualTo(16)
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(4)
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(2)
        assertThat(Status.getSavepointMode(cluster)).isEqualTo("Automatic")
        assertThat(Status.getRestartPolicy(cluster)).isEqualTo("Never")

        verify(controller, times(1)).updateStatus(eq(clusterSelector), eq(cluster))
        verify(controller, times(1)).updateFinalizers(eq(clusterSelector), eq(cluster))
        verify(controller, times(1)).updateAnnotations(eq(clusterSelector), eq(cluster))
    }

    @Test
    fun `should create bootstrap resource`() {
        context.initializeStatus()
        given(controller.createBootstrapJob(eq(clusterSelector), any())).thenReturn(OperationResult(status = OperationStatus.OK, output = "xxx"))
        assertThat(context.createBootstrapJob(clusterSelector)).isNotNull()
        verify(controller, times(1)).createBootstrapJob(eq(clusterSelector), any())
    }

    @Test
    fun `should create bootstrap resource without savepoint`() {
        Annotations.setWithoutSavepoint(cluster, true)
        context.initializeStatus()
        given(controller.createBootstrapJob(eq(clusterSelector), any())).thenReturn(OperationResult(status = OperationStatus.OK, output = "xxx"))
        assertThat(context.createBootstrapJob(clusterSelector)).isNotNull()
        //TODO why argThat doesn't work?
//        val jobCapture = CapturingMatcher<V1Job>()
        verify(controller, times(1)).createBootstrapJob(eq(clusterSelector), any())
//        assertThat(jobCapture.lastValue.spec.template.spec.containers[0].args.find { it == "SAVEPOINT_PATH" }).isNotNull()
    }

    @Test
    fun `should create jobmanager service resource`() {
        context.initializeStatus()
        given(controller.createService(eq(clusterSelector), any())).thenReturn(OperationResult(status = OperationStatus.OK, output = "xxx"))
        assertThat(context.createService(clusterSelector)).isNotNull()
        verify(controller, times(1)).createService(eq(clusterSelector), any())
    }

    @Test
    fun `should remove jar`() {
        given(controller.removeJar(eq(clusterSelector))).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        assertThat(context.removeJar(clusterSelector)).isNotNull()
        verify(controller, times(1)).removeJar(eq(clusterSelector))
    }

    @Test
    fun `should trigger savepoint`() {
        given(controller.triggerSavepoint(eq(clusterSelector), eq(savepointOptions))).thenReturn(OperationResult(status = OperationStatus.OK, output = savepointRequest))
        assertThat(context.triggerSavepoint(clusterSelector, savepointOptions)).isNotNull()
        verify(controller, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
    }

    @Test
    fun `should query savepoint`() {
        given(controller.querySavepoint(eq(clusterSelector), eq(savepointRequest))).thenReturn(OperationResult(status = OperationStatus.OK, output = "file:///tmp/1"))
        assertThat(context.querySavepoint(clusterSelector, savepointRequest)).isNotNull()
        verify(controller, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest))
    }

    @Test
    fun `should create job`() {
        val resource = TestFactory.aBootstrapJob(cluster)
        given(controller.createBootstrapJob(eq(clusterSelector), eq(resource))).thenReturn(OperationResult(status = OperationStatus.OK, output = "xxx"))
        assertThat(context.createBootstrapJob(clusterSelector, resource)).isNotNull()
        verify(controller, times(1)).createBootstrapJob(eq(clusterSelector), eq(resource))
    }

    @Test
    fun `should delete job`() {
        given(controller.deleteBootstrapJob(eq(clusterSelector))).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        assertThat(context.deleteBootstrapJob(clusterSelector)).isNotNull()
        verify(controller, times(1)).deleteBootstrapJob(eq(clusterSelector))
    }

    @Test
    fun `should create service`() {
        val resource = TestFactory.aJobManagerService(cluster)
        given(controller.createService(eq(clusterSelector), eq(resource))).thenReturn(OperationResult(status = OperationStatus.OK, output = "xxx"))
        assertThat(context.createService(clusterSelector, resource)).isNotNull()
        verify(controller, times(1)).createService(eq(clusterSelector), eq(resource))
    }

    @Test
    fun `should delete service`() {
        given(controller.deleteService(eq(clusterSelector))).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        assertThat(context.deleteService(clusterSelector)).isNotNull()
        verify(controller, times(1)).deleteService(eq(clusterSelector))
    }

    @Test
    fun `should create pods`() {
        given(controller.createPods(eq(clusterSelector), eq(podReplicas))).thenReturn(OperationResult(status = OperationStatus.OK, output = setOf("1", "2")))
        assertThat(context.createPods(clusterSelector, podReplicas)).isNotNull()
        verify(controller, times(1)).createPods(eq(clusterSelector), eq(podReplicas))
    }

    @Test
    fun `should delete pod`() {
        given(controller.deletePods(eq(clusterSelector), any())).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        assertThat(context.deletePods(clusterSelector, deleteOptions)).isNotNull()
        verify(controller, times(1)).deletePods(eq(clusterSelector), eq(deleteOptions))
    }

    @Test
    fun `should verify that pods are running`() {
        given(controller.arePodsRunning(eq(clusterSelector))).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        assertThat(context.arePodsRunning(clusterSelector)).isNotNull()
        verify(controller, times(1)).arePodsRunning(eq(clusterSelector))
    }

    @Test
    fun `should verify that pods are terminated`() {
        given(controller.arePodsTerminated(eq(clusterSelector))).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        assertThat(context.arePodsTerminated(clusterSelector)).isNotNull()
        verify(controller, times(1)).arePodsTerminated(eq(clusterSelector))
    }


    @Test
    fun `should start job`() {
        given(controller.startJob(eq(clusterSelector), eq(cluster))).thenReturn(OperationResult(status = OperationStatus.OK, output = null))
        assertThat(context.startJob(clusterSelector, cluster)).isNotNull()
        verify(controller, times(1)).startJob(eq(clusterSelector), eq(cluster))
    }

    @Test
    fun `should stop job`() {
        given(controller.stopJob(eq(clusterSelector))).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        assertThat(context.stopJob(clusterSelector)).isNotNull()
        verify(controller, times(1)).stopJob(eq(clusterSelector))
    }

    @Test
    fun `should cancel job`() {
        given(controller.cancelJob(eq(clusterSelector), eq(savepointOptions))).thenReturn(OperationResult(status = OperationStatus.OK, output = savepointRequest))
        assertThat(context.cancelJob(clusterSelector, savepointOptions)).isNotNull()
        verify(controller, times(1)).cancelJob(eq(clusterSelector), eq(savepointOptions))
    }

    @Test
    fun `should verify that cluster is ready`() {
        given(controller.isClusterReady(eq(clusterSelector), eq(clusterScale))).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        assertThat(context.isClusterReady(clusterSelector, clusterScale)).isNotNull()
        verify(controller, times(1)).isClusterReady(eq(clusterSelector), eq(clusterScale))
    }

    @Test
    fun `should verify that job is running`() {
        given(controller.isJobRunning(eq(clusterSelector))).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        assertThat(context.isJobRunning(clusterSelector)).isNotNull()
        verify(controller, times(1)).isJobRunning(eq(clusterSelector))
    }

    @Test
    fun `should verify that job is finished`() {
        given(controller.isJobFinished(eq(clusterSelector))).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        assertThat(context.isJobFinished(clusterSelector)).isNotNull()
        verify(controller, times(1)).isJobFinished(eq(clusterSelector))
    }

    @Test
    fun `should verify that job is failed`() {
        given(controller.isJobFailed(eq(clusterSelector))).thenReturn(OperationResult(status = OperationStatus.OK, output = true))
        assertThat(context.isJobFailed(clusterSelector)).isNotNull()
        verify(controller, times(1)).isJobFailed(eq(clusterSelector))
    }

    @Test
    fun `should return time passed since last update`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 5000)
        assertThat(context.timeSinceLastUpdateInSeconds()).isGreaterThanOrEqualTo(5)
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 10000)
        assertThat(context.timeSinceLastUpdateInSeconds()).isGreaterThanOrEqualTo(10)
    }

    @Test
    fun `should return time passed since last savepoint request`() {
        Status.setSavepointRequest(cluster, savepointRequest)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 5000)
        assertThat(context.timeSinceLastSavepointRequestInSeconds()).isGreaterThanOrEqualTo(5)
        Status.setSavepointRequest(cluster, savepointRequest)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 10000)
        assertThat(context.timeSinceLastSavepointRequestInSeconds()).isGreaterThanOrEqualTo(10)
    }
}
