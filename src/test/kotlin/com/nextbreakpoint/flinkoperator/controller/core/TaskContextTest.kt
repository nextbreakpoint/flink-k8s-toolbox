package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import io.kubernetes.client.models.V1StatefulSetStatus
import org.assertj.core.api.Assertions.assertThat
import org.joda.time.DateTime
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify

class TaskContextTest {
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val clusterId = ClusterId(name = "test", namespace = "flink", uuid = "123")
    private val resources = CachedResources(
        flinkCluster = cluster,
        bootstrapJob = TestFactory.aBootstrapJob(cluster),
        jobmanagerService = TestFactory.aJobManagerService(cluster),
        jobmanagerStatefulSet = TestFactory.aJobManagerStatefulSet(cluster),
        taskmanagerStatefulSet = TestFactory.aTaskManagerStatefulSet(cluster),
        jobmanagerPVC = TestFactory.aJobManagerPersistenVolumeClaim(cluster),
        taskmanagerPVC = TestFactory.aTaskManagerPersistenVolumeClaim(cluster)
    )
    private val controller = mock(OperationController::class.java)
    private val context = TaskContext(clusterId, cluster, resources, controller)
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val savepointOptions = SavepointOptions(targetPath = "file:///tmp")
    private val clusterScaling = ClusterScaling(taskManagers = 1, taskSlots = 1)

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
        assertThat(context.getSavepointOtions()).isEqualTo(savepointOptions)
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
        assertThat(Status.getJobRestartPolicy(cluster)).isNull()
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
        assertThat(Status.getJobRestartPolicy(cluster)).isEqualTo("Never")
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
        assertThat(context.getClusterScale()).isEqualTo(ClusterScaling(taskSlots = 0, taskManagers = 0))
        Status.setTaskManagers(cluster, 4)
        Status.setTaskSlots(cluster, 2)
        assertThat(context.getClusterScale()).isEqualTo(ClusterScaling(taskSlots = 2, taskManagers = 4))
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
        assertThat(context.getJobRestartPolicy()).isNull()
        Status.setJobRestartPolicy(cluster, "Never")
        assertThat(context.getJobRestartPolicy()).isEqualTo("Never")
    }

    @Test
    fun `should return desired number of task managers`() {
        assertThat(context.getDesiredTaskManagers()).isEqualTo(1)
        cluster.spec?.taskManagers = 4
        assertThat(context.getDesiredTaskManagers()).isEqualTo(4)
    }

    @Test
    fun `should return number of jobmanager replicas`() {
        assertThat(context.getJobManagerReplicas()).isEqualTo(0)
        resources.jobmanagerStatefulSet?.status = V1StatefulSetStatus()
        resources.jobmanagerStatefulSet?.status?.replicas = 4
        assertThat(context.getJobManagerReplicas()).isEqualTo(4)
    }

    @Test
    fun `should return number of taskmanager replicas`() {
        assertThat(context.getTaskManagerReplicas()).isEqualTo(0)
        resources.taskmanagerStatefulSet?.status = V1StatefulSetStatus()
        resources.taskmanagerStatefulSet?.status?.replicas = 4
        assertThat(context.getTaskManagerReplicas()).isEqualTo(4)
    }

    @Test
    fun `should return true when batch mode otherwise false`() {
        context.initializeStatus()
        cluster.status?.bootstrap?.executionMode = "STREAM"
        assertThat(context.isBatchMode()).isFalse()
        cluster.status?.bootstrap?.executionMode = "BATCH"
        assertThat(context.isBatchMode()).isTrue()
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
        assertThat(context.doesBootstrapExists()).isTrue()
        val newResource = resources.withBootstrap(null)
        val newContext = TaskContext(clusterId, cluster, newResource, controller)
        assertThat(newContext.doesBootstrapExists()).isFalse()
    }

    @Test
    fun `should return true when jobmanager service exists otherwise false`() {
        assertThat(context.doesJobManagerServiceExists()).isTrue()
        val newResource = resources.withJobManagerService(null)
        val newContext = TaskContext(clusterId, cluster, newResource, controller)
        assertThat(newContext.doesJobManagerServiceExists()).isFalse()
    }

    @Test
    fun `should return true when jobmanager statefulset exists otherwise false`() {
        assertThat(context.doesJobManagerStatefulSetExists()).isTrue()
        val newResource = resources.withJobManagerStatefulSet(null)
        val newContext = TaskContext(clusterId, cluster, newResource, controller)
        assertThat(newContext.doesJobManagerStatefulSetExists()).isFalse()
    }

    @Test
    fun `should return true when taskmanager statefulset exists otherwise false`() {
        assertThat(context.doesTaskManagerStatefulSetExists()).isTrue()
        val newResource = resources.withTaskManagerStatefulSet(null)
        val newContext = TaskContext(clusterId, cluster, newResource, controller)
        assertThat(newContext.doesTaskManagerStatefulSetExists()).isFalse()
    }

    @Test
    fun `should return true when jobmanager persistent volume claim exists otherwise false`() {
        assertThat(context.doesJobManagerPVCExists()).isTrue()
        val newResource = resources.withJobManagerPVC(null)
        val newContext = TaskContext(clusterId, cluster, newResource, controller)
        assertThat(newContext.doesJobManagerPVCExists()).isFalse()
    }

    @Test
    fun `should return true when taskmanager persistent volume claim exists otherwise false`() {
        assertThat(context.doesTaskManagerPVCExists()).isTrue()
        val newResource = resources.withTaskManagerPVC(null)
        val newContext = TaskContext(clusterId, cluster, newResource, controller)
        assertThat(newContext.doesTaskManagerPVCExists()).isFalse()
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
        Status.setJobRestartPolicy(cluster, "Always")

        cluster.spec?.taskManagers = 2

        resources.taskmanagerStatefulSet?.status = V1StatefulSetStatus()
        resources.taskmanagerStatefulSet?.status?.readyReplicas = 4

        cluster.metadata.finalizers = listOf("finalizer.nextbreakpoint.com")

        val timestamp = System.currentTimeMillis()

        context.refreshStatus(DateTime(timestamp), DateTime(timestamp), false)

        assertThat(Status.getActiveTaskManagers(cluster)).isEqualTo(4)
        assertThat(Status.getTotalTaskSlots(cluster)).isEqualTo(16)
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(4)
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(2)
        assertThat(Status.getSavepointMode(cluster)).isEqualTo("Automatic")
        assertThat(Status.getJobRestartPolicy(cluster)).isEqualTo("Never")

        verify(controller, times(1)).updateStatus(eq(clusterId), eq(cluster))
        verify(controller, times(1)).updateFinalizers(eq(clusterId), eq(cluster))
        verify(controller, times(1)).updateAnnotations(eq(clusterId), eq(cluster))
    }

    @Test
    fun `should create bootstrap resource`() {
        context.initializeStatus()
        given(controller.createBootstrapJob(eq(clusterId), any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = "xxx"))
        assertThat(context.createBootstrapJob(clusterId)).isNotNull()
        verify(controller, times(1)).createBootstrapJob(eq(clusterId), any())
    }

    @Test
    fun `should create jobmanager service resource`() {
        context.initializeStatus()
        given(controller.createJobManagerService(eq(clusterId), any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = "xxx"))
        assertThat(context.createJobManagerService(clusterId)).isNotNull()
        verify(controller, times(1)).createJobManagerService(eq(clusterId), any())
    }

    @Test
    fun `should create jobmanager statefulset resource`() {
        context.initializeStatus()
        given(controller.createStatefulSet(eq(clusterId), any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = "xxx"))
        assertThat(context.createJobManagerStatefulSet(clusterId)).isNotNull()
        verify(controller, times(1)).createStatefulSet(eq(clusterId), any())
    }

    @Test
    fun `should create taskmanager statefulset resource`() {
        context.initializeStatus()
        given(controller.createStatefulSet(eq(clusterId), any())).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = "xxx"))
        assertThat(context.createTaskManagerStatefulSet(clusterId)).isNotNull()
        verify(controller, times(1)).createStatefulSet(eq(clusterId), any())
    }

    @Test
    fun `should remove jar`() {
        given(controller.removeJar(eq(clusterId))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.removeJar(clusterId)).isNotNull()
        verify(controller, times(1)).removeJar(eq(clusterId))
    }

    @Test
    fun `should trigger savepoint jar`() {
        given(controller.triggerSavepoint(eq(clusterId), eq(savepointOptions))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = savepointRequest))
        assertThat(context.triggerSavepoint(clusterId, savepointOptions)).isNotNull()
        verify(controller, times(1)).triggerSavepoint(eq(clusterId), eq(savepointOptions))
    }

    @Test
    fun `should get latest savepoint`() {
        given(controller.getLatestSavepoint(eq(clusterId), eq(savepointRequest))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = "file:///tmp/1"))
        assertThat(context.getLatestSavepoint(clusterId, savepointRequest)).isNotNull()
        verify(controller, times(1)).getLatestSavepoint(eq(clusterId), eq(savepointRequest))
    }

    @Test
    fun `should create job`() {
        val resource = TestFactory.aBootstrapJob(cluster)
        given(controller.createBootstrapJob(eq(clusterId), eq(resource))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = "xxx"))
        assertThat(context.createBootstrapJob(clusterId, resource)).isNotNull()
        verify(controller, times(1)).createBootstrapJob(eq(clusterId), eq(resource))
    }

    @Test
    fun `should delete job`() {
        given(controller.deleteBootstrapJob(eq(clusterId))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.deleteBootstrapJob(clusterId)).isNotNull()
        verify(controller, times(1)).deleteBootstrapJob(eq(clusterId))
    }

    @Test
    fun `should create service`() {
        val resource = TestFactory.aJobManagerService(cluster)
        given(controller.createJobManagerService(eq(clusterId), eq(resource))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = "xxx"))
        assertThat(context.createJobManagerService(clusterId, resource)).isNotNull()
        verify(controller, times(1)).createJobManagerService(eq(clusterId), eq(resource))
    }

    @Test
    fun `should delete service`() {
        given(controller.deleteJobManagerService(eq(clusterId))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.deleteJobManagerService(clusterId)).isNotNull()
        verify(controller, times(1)).deleteJobManagerService(eq(clusterId))
    }

    @Test
    fun `should create statefulset`() {
        val resource = TestFactory.aJobManagerStatefulSet(cluster)
        given(controller.createStatefulSet(eq(clusterId), eq(resource))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = "xxx"))
        assertThat(context.createStatefulSet(clusterId, resource)).isNotNull()
        verify(controller, times(1)).createStatefulSet(eq(clusterId), eq(resource))
    }

    @Test
    fun `should delete statefulsets`() {
        given(controller.deleteStatefulSets(eq(clusterId))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.deleteStatefulSets(clusterId)).isNotNull()
        verify(controller, times(1)).deleteStatefulSets(eq(clusterId))
    }

    @Test
    fun `should delete persistent volume claims`() {
        given(controller.deletePersistentVolumeClaims(eq(clusterId))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.deletePersistentVolumeClaims(clusterId)).isNotNull()
        verify(controller, times(1)).deletePersistentVolumeClaims(eq(clusterId))
    }

    @Test
    fun `should terminate pods`() {
        given(controller.terminatePods(eq(clusterId))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.terminatePods(clusterId)).isNotNull()
        verify(controller, times(1)).terminatePods(eq(clusterId))
    }

    @Test
    fun `should restart pods`() {
        given(controller.restartPods(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.restartPods(clusterId, clusterScaling)).isNotNull()
        verify(controller, times(1)).restartPods(eq(clusterId), eq(clusterScaling))
    }

    @Test
    fun `should verify that pods are terminated`() {
        given(controller.arePodsTerminated(eq(clusterId))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.arePodsTerminated(clusterId)).isNotNull()
        verify(controller, times(1)).arePodsTerminated(eq(clusterId))
    }

    @Test
    fun `should start job`() {
        given(controller.startJob(eq(clusterId), eq(cluster))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.startJob(clusterId, cluster)).isNotNull()
        verify(controller, times(1)).startJob(eq(clusterId), eq(cluster))
    }

    @Test
    fun `should stop job`() {
        given(controller.stopJob(eq(clusterId))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.stopJob(clusterId)).isNotNull()
        verify(controller, times(1)).stopJob(eq(clusterId))
    }

    @Test
    fun `should cancel job`() {
        given(controller.cancelJob(eq(clusterId), eq(savepointOptions))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = savepointRequest))
        assertThat(context.cancelJob(clusterId, savepointOptions)).isNotNull()
        verify(controller, times(1)).cancelJob(eq(clusterId), eq(savepointOptions))
    }

    @Test
    fun `should verify that cluster is ready`() {
        given(controller.isClusterReady(eq(clusterId), eq(clusterScaling))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.isClusterReady(clusterId, clusterScaling)).isNotNull()
        verify(controller, times(1)).isClusterReady(eq(clusterId), eq(clusterScaling))
    }

    @Test
    fun `should verify that job is running`() {
        given(controller.isJobRunning(eq(clusterId))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.isJobRunning(clusterId)).isNotNull()
        verify(controller, times(1)).isJobRunning(eq(clusterId))
    }

    @Test
    fun `should verify that job is finished`() {
        given(controller.isJobFinished(eq(clusterId))).thenReturn(OperationResult(status = OperationStatus.COMPLETED, output = null))
        assertThat(context.isJobFinished(clusterId)).isNotNull()
        verify(controller, times(1)).isJobFinished(eq(clusterId))
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
