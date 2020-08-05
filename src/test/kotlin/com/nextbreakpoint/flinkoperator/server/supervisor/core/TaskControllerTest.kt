package com.nextbreakpoint.flinkoperator.server.supervisor.core

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.ClusterScale
import com.nextbreakpoint.flinkoperator.common.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.DeleteOptions
import com.nextbreakpoint.flinkoperator.common.ManualAction
import com.nextbreakpoint.flinkoperator.common.PodReplicas
import com.nextbreakpoint.flinkoperator.common.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.SavepointRequest
import com.nextbreakpoint.flinkoperator.server.controller.Controller
import com.nextbreakpoint.flinkoperator.server.common.Annotations
import com.nextbreakpoint.flinkoperator.server.common.Configuration
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import com.nextbreakpoint.flinkoperator.server.common.Status
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

class TaskControllerTest {
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val clusterSelector = ClusterSelector(name = "test", namespace = "flink", uuid = "123")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val savepointOptions = SavepointOptions(targetPath = "file:///tmp")
    private val podReplicas = PodReplicas(pod = TestFactory.aTaskManagerPod(cluster, "1"), replicas = 2)
    private val clusterScale = ClusterScale(taskManagers = 2, taskSlots = 2)
    private val deleteOptions = DeleteOptions(label = "role", value = "jobmanager", limit = 1)
    private val resources = CachedResources(
        flinkCluster = cluster,
        bootstrapJob = TestFactory.aBootstrapJob(cluster),
        service = TestFactory.aJobManagerService(cluster),
        jobmanagerPods = setOf(TestFactory.aJobManagerPod(cluster, "1")),
        taskmanagerPods = setOf(TestFactory.aTaskManagerPod(cluster, "1"))
    )
    private val logger = mock(Logger::class.java)
    private val controller = mock(Controller::class.java)
    private val taskController = TaskController(clusterSelector, cluster, resources, controller)

    @Test
    fun `should update savepoint request`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        val savepointRequest = savepointRequest
        assertThat(Status.getSavepointRequest(cluster)).isNull()
        taskController.setSavepointRequest(savepointRequest)
        assertThat(Status.getSavepointRequest(cluster)).isEqualTo(savepointRequest)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return savepoint request`() {
        val savepointRequest = savepointRequest
        assertThat(taskController.getSavepointRequest()).isNull()
        Status.setSavepointRequest(cluster, savepointRequest)
        assertThat(Status.getSavepointRequest(cluster)).isEqualTo(savepointRequest)
    }

    @Test
    fun `should reset savepoint request`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        val savepointRequest = savepointRequest
        Status.setSavepointRequest(cluster, savepointRequest)
        taskController.resetSavepointRequest()
        assertThat(Status.getSavepointRequest(cluster)).isNull()
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update savepoint path`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getSavepointPath(cluster)).isNull()
        taskController.setSavepointPath("file:///tmp/1")
        assertThat(Status.getSavepointPath(cluster)).isEqualTo("file:///tmp/1")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update cluster status`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Unknown)
        taskController.setClusterStatus(ClusterStatus.Running)
        assertThat(taskController.getClusterStatus()).isEqualTo(ClusterStatus.Running)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Running)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return cluster status`() {
        assertThat(taskController.getClusterStatus()).isEqualTo(ClusterStatus.Unknown)
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        assertThat(taskController.getClusterStatus()).isEqualTo(ClusterStatus.Running)
    }

    @Test
    fun `should update delete resources`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Annotations.getActionTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Annotations.isDeleteResources(cluster)).isEqualTo(false)
        taskController.setDeleteResources(true)
        assertThat(Annotations.isDeleteResources(cluster)).isEqualTo(true)
        assertThat(Annotations.getActionTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should reset manual action`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Annotations.getActionTimestamp(cluster)).isEqualTo(DateTime(0))
        Annotations.setManualAction(cluster, ManualAction.STOP)
        taskController.resetManualAction()
        assertThat(Annotations.getManualAction(cluster)).isEqualTo(ManualAction.NONE)
        assertThat(Annotations.getActionTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return savepoint options`() {
        cluster.spec?.operator?.savepointTargetPath = "file:///tmp"
        val savepointOptions = savepointOptions
        assertThat(taskController.getSavepointOptions()).isEqualTo(savepointOptions)
    }

    @Test
    fun `should return savepoint interval`() {
        assertThat(taskController.getSavepointInterval()).isEqualTo(60)
        cluster.spec?.operator?.savepointInterval = 200
        assertThat(taskController.getSavepointInterval()).isEqualTo(200)
    }

    @Test
    fun `should return savepoint mode`() {
        Status.setSavepointMode(cluster, "Manual")
        assertThat(taskController.getSavepointMode()).isEqualTo("Manual")
    }

    @Test
    fun `should return has been deleted`() {
        assertThat(taskController.hasBeenDeleted()).isEqualTo(false)
        cluster.metadata.deletionTimestamp = DateTime(System.currentTimeMillis())
        assertThat(taskController.hasBeenDeleted()).isEqualTo(true)
    }

    @Test
    fun `should return has finalizer`() {
        assertThat(taskController.hasFinalizer()).isEqualTo(false)
        cluster.metadata.finalizers = listOf("finalizer.nextbreakpoint.com")
        assertThat(taskController.hasFinalizer()).isEqualTo(true)
    }

    @Test
    fun `should add finalizer`() {
        assertThat(taskController.hasFinalizer()).isEqualTo(false)
        taskController.addFinalizer()
        assertThat(taskController.hasFinalizer()).isEqualTo(true)
    }

    @Test
    fun `should remove finalizer`() {
        cluster.metadata.finalizers = listOf("finalizer.nextbreakpoint.com")
        assertThat(taskController.hasFinalizer()).isEqualTo(true)
        taskController.removeFinalizer()
        assertThat(taskController.hasFinalizer()).isEqualTo(false)
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
        taskController.initializeStatus()
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
        taskController.initializeAnnotations()
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
        taskController.updateDigests()
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
        taskController.updateStatus()
        assertThat(Status.getBootstrap(cluster)).isNotNull()
        assertThat(Status.getServiceMode(cluster)).isNotNull()
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(2)
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(3)
        assertThat(Status.getJobParallelism(cluster)).isEqualTo(6)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return no changes when digests didn't change`() {
        taskController.updateDigests()
        val changes = taskController.computeChanges()
        assertThat(changes).isEmpty()
    }

    @Test
    fun `should return one change when bootstrap digest changed`() {
        taskController.updateDigests()
        Status.setBootstrapDigest(cluster, "0")
        val changes = taskController.computeChanges()
        assertThat(changes).containsExactly("BOOTSTRAP")
    }

    @Test
    fun `should return one change when runtime digest changed`() {
        taskController.updateDigests()
        Status.setRuntimeDigest(cluster, "0")
        val changes = taskController.computeChanges()
        assertThat(changes).containsExactly("RUNTIME")
    }

    @Test
    fun `should return one change when jobmanager digest changed`() {
        taskController.updateDigests()
        Status.setJobManagerDigest(cluster, "0")
        val changes = taskController.computeChanges()
        assertThat(changes).containsExactly("JOB_MANAGER")
    }

    @Test
    fun `should return one change when taskmanager digest changed`() {
        taskController.updateDigests()
        Status.setTaskManagerDigest(cluster, "0")
        val changes = taskController.computeChanges()
        assertThat(changes).containsExactly("TASK_MANAGER")
    }

    @Test
    fun `should return bootstrap present`() {
        taskController.updateStatus()
        assertThat(taskController.isBootstrapPresent()).isTrue()
        Status.setBootstrap(cluster, null)
        assertThat(taskController.isBootstrapPresent()).isFalse()
    }

    @Test
    fun `should rescale cluster`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        Status.setTaskSlots(cluster, 2)
        cluster.spec?.taskManagers = 4
        taskController.rescaleCluster()
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(4)
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(2)
        assertThat(Status.getJobParallelism(cluster)).isEqualTo(8)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return number of task managers`() {
        assertThat(taskController.getTaskManagers()).isEqualTo(0)
        Status.setTaskManagers(cluster, 4)
        assertThat(taskController.getTaskManagers()).isEqualTo(4)
    }

    @Test
    fun `should return cluster scale`() {
        assertThat(taskController.getClusterScale()).isEqualTo(ClusterScale(taskSlots = 0, taskManagers = 0))
        Status.setTaskManagers(cluster, 4)
        Status.setTaskSlots(cluster, 2)
        assertThat(taskController.getClusterScale()).isEqualTo(ClusterScale(taskSlots = 2, taskManagers = 4))
    }

    @Test
    fun `should return action timestamp`() {
        assertThat(taskController.getActionTimestamp()).isEqualTo(Annotations.getActionTimestamp(cluster))
        Annotations.setManualAction(cluster, ManualAction.STOP)
        assertThat(taskController.getActionTimestamp()).isEqualTo(Annotations.getActionTimestamp(cluster))
    }

    @Test
    fun `should return status timestamp`() {
        assertThat(taskController.getStatusTimestamp()).isEqualTo(Status.getStatusTimestamp(cluster))
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        assertThat(taskController.getStatusTimestamp()).isEqualTo(Status.getStatusTimestamp(cluster))
    }

    @Test
    fun `should return manual action`() {
        assertThat(taskController.getManualAction()).isEqualTo(ManualAction.NONE)
        Annotations.setManualAction(cluster, ManualAction.STOP)
        assertThat(taskController.getManualAction()).isEqualTo(ManualAction.STOP)
    }

    @Test
    fun `should return job restart policy`() {
        assertThat(taskController.getRestartPolicy()).isNull()
        Status.setRestartPolicy(cluster, "Never")
        assertThat(taskController.getRestartPolicy()).isEqualTo("Never")
    }

    @Test
    fun `should return desired number of task managers`() {
        assertThat(taskController.getDesiredTaskManagers()).isEqualTo(1)
        cluster.spec?.taskManagers = 4
        assertThat(taskController.getDesiredTaskManagers()).isEqualTo(4)
    }

    @Test
    fun `should return number of jobmanager replicas`() {
        val resources = resources.withJobManagerPods(setOf(
            TestFactory.aJobManagerPod(cluster,"1"),
            TestFactory.aJobManagerPod(cluster,"2")
        ))
        val context = TaskController(clusterSelector, cluster, resources, controller)
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
        val context = TaskController(clusterSelector, cluster, resources, controller)
        assertThat(context.getTaskManagerReplicas()).isEqualTo(4)
    }

    @Test
    fun `should return true when resource deleted otherwise false`() {
        Annotations.setDeleteResources(cluster, false)
        assertThat(taskController.isDeleteResources()).isFalse()
        Annotations.setDeleteResources(cluster, true)
        assertThat(taskController.isDeleteResources()).isTrue()
    }

    @Test
    fun `should return true when bootstrap exists otherwise false`() {
        assertThat(taskController.doesBootstrapJobExists()).isTrue()
        val newResource = resources.withBootstrapJob(null)
        val newController = TaskController(clusterSelector, cluster, newResource, controller)
        assertThat(newController.doesBootstrapJobExists()).isFalse()
    }

    @Test
    fun `should return true when jobmanager service exists otherwise false`() {
        assertThat(taskController.doesServiceExists()).isTrue()
        val newResource = resources.withService(null)
        val newController = TaskController(clusterSelector, cluster, newResource, controller)
        assertThat(newController.doesServiceExists()).isFalse()
    }

    @Test
    fun `should return true when jobmanager pod exists otherwise false`() {
        assertThat(taskController.doesJobManagerPodsExists()).isTrue()
        val newResource = resources.withJobManagerPods(setOf())
        val newController = TaskController(clusterSelector, cluster, newResource, controller)
        assertThat(newController.doesJobManagerPodsExists()).isFalse()
    }

    @Test
    fun `should return true when taskmanager pod exists otherwise false`() {
        assertThat(taskController.doesTaskManagerPodsExists()).isTrue()
        val newResource = resources.withTaskManagerPods(setOf())
        val newController = TaskController(clusterSelector, cluster, newResource, controller)
        assertThat(newController.doesTaskManagerPodsExists()).isFalse()
    }

    @Test
    fun `should return true when savepoint is required otherwise false`() {
        val currentTimeMillis = System.currentTimeMillis()
        Status.setSavepointRequest(cluster, savepointRequest)
        val savepointInterval = Configuration.getSavepointInterval(cluster)
        given(controller.currentTimeMillis()).thenReturn(currentTimeMillis + savepointInterval * 1000 - 1)
        Annotations.setWithoutSavepoint(cluster, false)
        Annotations.setDeleteResources(cluster, false)
        assertThat(taskController.isSavepointRequired()).isEqualTo(false)
        given(controller.currentTimeMillis()).thenReturn(currentTimeMillis + savepointInterval * 1000 + 10000)
        Annotations.setWithoutSavepoint(cluster, false)
        Annotations.setDeleteResources(cluster, false)
        assertThat(taskController.isSavepointRequired()).isEqualTo(true)
        Annotations.setWithoutSavepoint(cluster, true)
        Annotations.setDeleteResources(cluster, false)
        assertThat(taskController.isSavepointRequired()).isEqualTo(false)
        Annotations.setWithoutSavepoint(cluster, false)
        Annotations.setDeleteResources(cluster, true)
        assertThat(taskController.isSavepointRequired()).isEqualTo(false)
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
        val context = TaskController(clusterSelector, cluster, resources, controller)

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
        taskController.initializeStatus()
        given(controller.createBootstrapJob(eq(clusterSelector), any())).thenReturn(Result(status = ResultStatus.OK, output = "xxx"))
        assertThat(taskController.createBootstrapJob(clusterSelector)).isNotNull()
        verify(controller, times(1)).createBootstrapJob(eq(clusterSelector), any())
    }

    @Test
    fun `should create bootstrap resource without savepoint`() {
        Annotations.setWithoutSavepoint(cluster, true)
        taskController.initializeStatus()
        given(controller.createBootstrapJob(eq(clusterSelector), any())).thenReturn(Result(status = ResultStatus.OK, output = "xxx"))
        assertThat(taskController.createBootstrapJob(clusterSelector)).isNotNull()
        //TODO why argThat doesn't work?
//        val jobCapture = CapturingMatcher<V1Job>()
        verify(controller, times(1)).createBootstrapJob(eq(clusterSelector), any())
//        assertThat(jobCapture.lastValue.spec.template.spec.containers[0].args.find { it == "SAVEPOINT_PATH" }).isNotNull()
    }

    @Test
    fun `should create jobmanager service resource`() {
        taskController.initializeStatus()
        given(controller.createService(eq(clusterSelector), any())).thenReturn(Result(status = ResultStatus.OK, output = "xxx"))
        assertThat(taskController.createService(clusterSelector)).isNotNull()
        verify(controller, times(1)).createService(eq(clusterSelector), any())
    }

    @Test
    fun `should remove jar`() {
        given(controller.removeJar(eq(clusterSelector))).thenReturn(Result(status = ResultStatus.OK, output = null))
        assertThat(taskController.removeJar(clusterSelector)).isNotNull()
        verify(controller, times(1)).removeJar(eq(clusterSelector))
    }

    @Test
    fun `should trigger savepoint`() {
        given(controller.triggerSavepoint(eq(clusterSelector), eq(savepointOptions))).thenReturn(Result(status = ResultStatus.OK, output = savepointRequest))
        assertThat(taskController.triggerSavepoint(clusterSelector, savepointOptions)).isNotNull()
        verify(controller, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions))
    }

    @Test
    fun `should query savepoint`() {
        given(controller.querySavepoint(eq(clusterSelector), eq(savepointRequest))).thenReturn(Result(status = ResultStatus.OK, output = "file:///tmp/1"))
        assertThat(taskController.querySavepoint(clusterSelector, savepointRequest)).isNotNull()
        verify(controller, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest))
    }

    @Test
    fun `should create job`() {
        val resource = TestFactory.aBootstrapJob(cluster)
        given(controller.createBootstrapJob(eq(clusterSelector), eq(resource))).thenReturn(Result(status = ResultStatus.OK, output = "xxx"))
        assertThat(taskController.createBootstrapJob(clusterSelector, resource)).isNotNull()
        verify(controller, times(1)).createBootstrapJob(eq(clusterSelector), eq(resource))
    }

    @Test
    fun `should delete job`() {
        given(controller.deleteBootstrapJob(eq(clusterSelector))).thenReturn(Result(status = ResultStatus.OK, output = null))
        assertThat(taskController.deleteBootstrapJob(clusterSelector)).isNotNull()
        verify(controller, times(1)).deleteBootstrapJob(eq(clusterSelector))
    }

    @Test
    fun `should create service`() {
        val resource = TestFactory.aJobManagerService(cluster)
        given(controller.createService(eq(clusterSelector), eq(resource))).thenReturn(Result(status = ResultStatus.OK, output = "xxx"))
        assertThat(taskController.createService(clusterSelector, resource)).isNotNull()
        verify(controller, times(1)).createService(eq(clusterSelector), eq(resource))
    }

    @Test
    fun `should delete service`() {
        given(controller.deleteService(eq(clusterSelector))).thenReturn(Result(status = ResultStatus.OK, output = null))
        assertThat(taskController.deleteService(clusterSelector)).isNotNull()
        verify(controller, times(1)).deleteService(eq(clusterSelector))
    }

    @Test
    fun `should create pods`() {
        given(controller.createPods(eq(clusterSelector), eq(podReplicas))).thenReturn(Result(status = ResultStatus.OK, output = setOf("1", "2")))
        assertThat(taskController.createPods(clusterSelector, podReplicas)).isNotNull()
        verify(controller, times(1)).createPods(eq(clusterSelector), eq(podReplicas))
    }

    @Test
    fun `should delete pod`() {
        given(controller.deletePods(eq(clusterSelector), any())).thenReturn(Result(status = ResultStatus.OK, output = null))
        assertThat(taskController.deletePods(clusterSelector, deleteOptions)).isNotNull()
        verify(controller, times(1)).deletePods(eq(clusterSelector), eq(deleteOptions))
    }

    @Test
    fun `should verify that pods are running`() {
        given(controller.arePodsRunning(eq(clusterSelector))).thenReturn(Result(status = ResultStatus.OK, output = true))
        assertThat(taskController.arePodsRunning(clusterSelector)).isNotNull()
        verify(controller, times(1)).arePodsRunning(eq(clusterSelector))
    }

    @Test
    fun `should verify that pods are terminated`() {
        given(controller.arePodsTerminated(eq(clusterSelector))).thenReturn(Result(status = ResultStatus.OK, output = true))
        assertThat(taskController.arePodsTerminated(clusterSelector)).isNotNull()
        verify(controller, times(1)).arePodsTerminated(eq(clusterSelector))
    }


    @Test
    fun `should start job`() {
        given(controller.startJob(eq(clusterSelector), eq(cluster))).thenReturn(Result(status = ResultStatus.OK, output = null))
        assertThat(taskController.startJob(clusterSelector, cluster)).isNotNull()
        verify(controller, times(1)).startJob(eq(clusterSelector), eq(cluster))
    }

    @Test
    fun `should stop job`() {
        given(controller.stopJob(eq(clusterSelector))).thenReturn(Result(status = ResultStatus.OK, output = true))
        assertThat(taskController.stopJob(clusterSelector)).isNotNull()
        verify(controller, times(1)).stopJob(eq(clusterSelector))
    }

    @Test
    fun `should cancel job`() {
        given(controller.cancelJob(eq(clusterSelector), eq(savepointOptions))).thenReturn(Result(status = ResultStatus.OK, output = savepointRequest))
        assertThat(taskController.cancelJob(clusterSelector, savepointOptions)).isNotNull()
        verify(controller, times(1)).cancelJob(eq(clusterSelector), eq(savepointOptions))
    }

    @Test
    fun `should verify that cluster is ready`() {
        given(controller.isClusterReady(eq(clusterSelector), eq(clusterScale))).thenReturn(Result(status = ResultStatus.OK, output = true))
        assertThat(taskController.isClusterReady(clusterSelector, clusterScale)).isNotNull()
        verify(controller, times(1)).isClusterReady(eq(clusterSelector), eq(clusterScale))
    }

    @Test
    fun `should verify that job is running`() {
        given(controller.isJobRunning(eq(clusterSelector))).thenReturn(Result(status = ResultStatus.OK, output = true))
        assertThat(taskController.isJobRunning(clusterSelector)).isNotNull()
        verify(controller, times(1)).isJobRunning(eq(clusterSelector))
    }

    @Test
    fun `should verify that job is finished`() {
        given(controller.isJobFinished(eq(clusterSelector))).thenReturn(Result(status = ResultStatus.OK, output = true))
        assertThat(taskController.isJobFinished(clusterSelector)).isNotNull()
        verify(controller, times(1)).isJobFinished(eq(clusterSelector))
    }

    @Test
    fun `should verify that job is failed`() {
        given(controller.isJobFailed(eq(clusterSelector))).thenReturn(Result(status = ResultStatus.OK, output = true))
        assertThat(taskController.isJobFailed(clusterSelector)).isNotNull()
        verify(controller, times(1)).isJobFailed(eq(clusterSelector))
    }

    @Test
    fun `should return time passed since last update`() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 5000)
        assertThat(taskController.timeSinceLastUpdateInSeconds()).isGreaterThanOrEqualTo(5)
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 10000)
        assertThat(taskController.timeSinceLastUpdateInSeconds()).isGreaterThanOrEqualTo(10)
    }

    @Test
    fun `should return time passed since last savepoint request`() {
        Status.setSavepointRequest(cluster, savepointRequest)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 5000)
        assertThat(taskController.timeSinceLastSavepointRequestInSeconds()).isGreaterThanOrEqualTo(5)
        Status.setSavepointRequest(cluster, savepointRequest)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 10000)
        assertThat(taskController.timeSinceLastSavepointRequestInSeconds()).isGreaterThanOrEqualTo(10)
    }
}
