package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.k8s.common.FlinkClusterAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.factory.ClusterResourcesDefaultFactory
import com.nextbreakpoint.flink.testing.KotlinMockito.any
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import com.nextbreakpoint.flink.testing.TestFactory
import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import com.nextbreakpoint.flinkclient.model.TaskManagersInfo
import io.kubernetes.client.openapi.models.V1PodStatus
import org.assertj.core.api.Assertions.assertThat
import org.joda.time.DateTime
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions
import java.util.logging.Logger

class ClusterControllerTest {
    private val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink", taskSlots = 2)
    private val job = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
    private val pod = TestFactory.aTaskManagerPod(cluster, "1")
    private val service = TestFactory.aJobManagerService(cluster)
    private val jobmanagerPods = mutableSetOf(TestFactory.aJobManagerPod(cluster, "1"))
    private val taskmanagerPods = mutableSetOf(TestFactory.aTaskManagerPod(cluster, "1"))
    private val resources = ClusterResources(
        flinkCluster = cluster,
        jobmanagerService = service,
        jobmanagerPods = jobmanagerPods,
        taskmanagerPods = taskmanagerPods,
        flinkJobs = setOf(job)
    )
    private val logger = mock(Logger::class.java)
    private val controller = mock(Controller::class.java)
    private val clusterController = ClusterController("flink", "test", 5, controller, resources, cluster)

    @AfterEach
    fun verifyInteractions() {
        verifyNoMoreInteractions(controller)
    }

    @Test
    fun `should return time passed since last update`() {
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Started)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 5000)
        assertThat(clusterController.timeSinceLastUpdateInSeconds()).isGreaterThanOrEqualTo(5)
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Started)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 10000)
        assertThat(clusterController.timeSinceLastUpdateInSeconds()).isGreaterThanOrEqualTo(10)
        verify(controller, times(2)).currentTimeMillis()
    }

    @Test
    fun `should return time passed since last rescale`() {
        FlinkClusterStatus.setTaskManagers(cluster, 4)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 5000)
        assertThat(clusterController.timeSinceLastRescaleInSeconds()).isGreaterThanOrEqualTo(5)
        FlinkClusterStatus.setTaskManagers(cluster, 2)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 10000)
        assertThat(clusterController.timeSinceLastRescaleInSeconds()).isGreaterThanOrEqualTo(10)
        verify(controller, times(2)).currentTimeMillis()
    }

    @Test
    fun `should remove jars`() {
        val result: Result<Void?> = Result(status = ResultStatus.OK, output = null)
        given(controller.removeJars(eq("flink"), eq("test"))).thenReturn(result)
        assertThat(clusterController.removeJars()).isEqualTo(result)
        verify(controller, times(1)).removeJars(eq("flink"), eq("test"))
    }

    @Test
    fun `should create pod`() {
        val result: Result<String?> = Result(status = ResultStatus.OK, output = "podname")
        given(controller.createPod(eq("flink"), eq("test"), eq(pod))).thenReturn(result)
        assertThat(clusterController.createPod(pod)).isEqualTo(result)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).createPod(eq("flink"), eq("test"), eq(pod))
    }

    @Test
    fun `should delete pod`() {
        val result: Result<Void?> = Result(status = ResultStatus.OK, output = null)
        given(controller.deletePod(eq("flink"), eq("test"), any())).thenReturn(result)
        assertThat(clusterController.deletePod("podname")).isEqualTo(result)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).deletePod(eq("flink"), eq("test"), eq("podname"))
    }

    @Test
    fun `should delete service`() {
        val result: Result<Void?> = Result(status = ResultStatus.OK, output = null)
        given(controller.deleteService(eq("flink"), eq("test"), eq("jobmanager-test"))).thenReturn(result)
        assertThat(clusterController.deleteService()).isEqualTo(result)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).deleteService(eq("flink"), eq("test"), eq("jobmanager-test"))
    }

    @Test
    fun `should stop jobs`() {
        val result: Result<Boolean> = Result(status = ResultStatus.OK, output = true)
        given(controller.stopJobs(eq("flink"), eq("test"), eq(setOf("a","b")))).thenReturn(result)
        assertThat(clusterController.stopJobs(setOf("a","b"))).isEqualTo(result)
        verify(controller, times(1)).stopJobs(eq("flink"), eq("test"), eq(setOf("a","b")))
    }

    @Test
    fun `should verify if cluster is ready`() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
        FlinkJobStatus.setJobParallelism(job, 2)
        val result: Result<Boolean> = Result(status = ResultStatus.OK, output = true)
        given(controller.isClusterReady(eq("flink"), eq("test"), eq(2))).thenReturn(result)
        assertThat(clusterController.isClusterReady()).isEqualTo(result)
        verify(controller, times(1)).isClusterReady(eq("flink"), eq("test"), eq(2))
    }

    @Test
    fun `should verify if cluster is healthy`() {
        val result: Result<Boolean> = Result(status = ResultStatus.OK, output = true)
        given(controller.isClusterHealthy(eq("flink"), eq("test"))).thenReturn(result)
        assertThat(clusterController.isClusterHealthy()).isEqualTo(result)
        verify(controller, times(1)).isClusterHealthy(eq("flink"), eq("test"))
    }

    @Test
    fun `should refresh status`() {
        FlinkClusterStatus.setTaskManagerReplicas(cluster, 0)
        FlinkClusterStatus.setTotalTaskSlots(cluster, 0)
        FlinkClusterStatus.setTaskManagers(cluster, 2)

        taskmanagerPods.clear()
        taskmanagerPods.add(TestFactory.aTaskManagerPod(cluster,"1"))
        taskmanagerPods.add(TestFactory.aTaskManagerPod(cluster,"2"))

        cluster.metadata.finalizers = listOf("supervisor.nextbreakpoint.com")

        val timestamp = System.currentTimeMillis()

        clusterController.refreshStatus(logger, DateTime(timestamp), DateTime(timestamp), false)

        assertThat(FlinkClusterStatus.getTaskManagerReplicas(cluster)).isEqualTo(2)
        assertThat(FlinkClusterStatus.getTotalTaskSlots(cluster)).isEqualTo(4)
        assertThat(FlinkClusterStatus.getTaskManagers(cluster)).isEqualTo(1)

        verify(controller, times(1)).updateStatus(eq("flink"), eq("test"), eq(cluster))
        verify(controller, times(1)).updateFinalizers(eq("flink"), eq("test"), eq(cluster))
        verify(controller, times(1)).updateAnnotations(eq("flink"), eq("test"), eq(cluster))
    }

    @Test
    fun `should return true when resource has been deleted`() {
        assertThat(clusterController.hasBeenDeleted()).isEqualTo(false)
        cluster.metadata.deletionTimestamp = DateTime(System.currentTimeMillis())
        assertThat(clusterController.hasBeenDeleted()).isEqualTo(true)
    }

    @Test
    fun `should return true when resource has finalizer`() {
        assertThat(clusterController.hasFinalizer()).isEqualTo(false)
        cluster.metadata.finalizers = listOf("supervisor.nextbreakpoint.com")
        assertThat(clusterController.hasFinalizer()).isEqualTo(true)
    }

    @Test
    fun `should add finalizer`() {
        assertThat(clusterController.hasFinalizer()).isEqualTo(false)
        clusterController.addFinalizer()
        assertThat(clusterController.hasFinalizer()).isEqualTo(true)
    }

    @Test
    fun `should remove finalizer`() {
        cluster.metadata.finalizers = listOf("supervisor.nextbreakpoint.com")
        assertThat(clusterController.hasFinalizer()).isEqualTo(true)
        clusterController.removeFinalizer()
        assertThat(clusterController.hasFinalizer()).isEqualTo(false)
    }

    @Test
    fun `should initialize status`() {
        assertThat(FlinkClusterStatus.getLabelSelector(cluster)).isNull()
        assertThat(FlinkClusterStatus.getServiceMode(cluster)).isNull()
        assertThat(FlinkClusterStatus.getTaskManagers(cluster)).isEqualTo(0)
        assertThat(FlinkClusterStatus.getTaskSlots(cluster)).isNull()
        cluster.spec?.taskManagers = 1
        cluster.spec?.taskManager?.taskSlots = 2
        clusterController.initializeStatus()
        assertThat(FlinkClusterStatus.getLabelSelector(cluster)).isNotEmpty()
        assertThat(FlinkClusterStatus.getServiceMode(cluster)).isEqualTo("ClusterIP")
        assertThat(FlinkClusterStatus.getTaskManagers(cluster)).isEqualTo(1)
        assertThat(FlinkClusterStatus.getTaskSlots(cluster)).isEqualTo(2)
    }

    @Test
    fun `should initialize annotations`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isLessThan(timestamp)
        FlinkClusterAnnotations.setRequestedAction(cluster, Action.STOP)
        FlinkClusterAnnotations.setDeleteResources(cluster, true)
        FlinkClusterAnnotations.setWithoutSavepoint(cluster, true)
        clusterController.initializeAnnotations()
        assertThat(FlinkClusterAnnotations.getRequestedAction(cluster)).isEqualTo(Action.NONE)
        assertThat(FlinkClusterAnnotations.isDeleteResources(cluster)).isFalse()
        assertThat(FlinkClusterAnnotations.isWithoutSavepoint(cluster)).isFalse()
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update digests`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isLessThan(timestamp)
        assertThat(FlinkClusterStatus.getRuntimeDigest(cluster)).isNull()
        assertThat(FlinkClusterStatus.getJobManagerDigest(cluster)).isNull()
        assertThat(FlinkClusterStatus.getTaskManagerDigest(cluster)).isNull()
        clusterController.updateDigests()
        assertThat(FlinkClusterStatus.getRuntimeDigest(cluster)).isNotNull()
        assertThat(FlinkClusterStatus.getJobManagerDigest(cluster)).isNotNull()
        assertThat(FlinkClusterStatus.getTaskManagerDigest(cluster)).isNotNull()
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update status`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isLessThan(timestamp)
        assertThat(FlinkClusterStatus.getServiceMode(cluster)).isNull()
        assertThat(FlinkClusterStatus.getTaskManagers(cluster)).isEqualTo(0)
        assertThat(FlinkClusterStatus.getTaskSlots(cluster)).isNull()
        cluster.spec?.taskManagers = 2
        cluster.spec?.taskManager?.taskSlots = 3
        clusterController.updateStatus()
        assertThat(FlinkClusterStatus.getServiceMode(cluster)).isNotNull()
        assertThat(FlinkClusterStatus.getTaskManagers(cluster)).isEqualTo(2)
        assertThat(FlinkClusterStatus.getTaskSlots(cluster)).isEqualTo(3)
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return no changes when digests didn't change`() {
        clusterController.updateDigests()
        val changes = clusterController.computeChanges()
        assertThat(changes).isEmpty()
    }

    @Test
    fun `should return one change when runtime digest changed`() {
        clusterController.updateDigests()
        FlinkClusterStatus.setRuntimeDigest(cluster, "0")
        val changes = clusterController.computeChanges()
        assertThat(changes).containsExactly("RUNTIME")
    }

    @Test
    fun `should return one change when jobmanager digest changed`() {
        clusterController.updateDigests()
        FlinkClusterStatus.setJobManagerDigest(cluster, "0")
        val changes = clusterController.computeChanges()
        assertThat(changes).containsExactly("JOB_MANAGER")
    }

    @Test
    fun `should return one change when taskmanager digest changed`() {
        clusterController.updateDigests()
        FlinkClusterStatus.setTaskManagerDigest(cluster, "0")
        val changes = clusterController.computeChanges()
        assertThat(changes).containsExactly("TASK_MANAGER")
    }

    @Test
    fun `should update supervisor status`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isLessThan(timestamp)
        assertThat(FlinkClusterStatus.getSupervisorStatus(cluster)).isEqualTo(ClusterStatus.Unknown)
        clusterController.setSupervisorStatus(ClusterStatus.Started)
        assertThat(clusterController.getSupervisorStatus()).isEqualTo(ClusterStatus.Started)
        assertThat(FlinkClusterStatus.getSupervisorStatus(cluster)).isEqualTo(ClusterStatus.Started)
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return supervisor status`() {
        assertThat(clusterController.getSupervisorStatus()).isEqualTo(ClusterStatus.Unknown)
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Started)
        assertThat(clusterController.getSupervisorStatus()).isEqualTo(ClusterStatus.Started)
    }

    @Test
    fun `should update resource status`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isLessThan(timestamp)
        assertThat(FlinkClusterStatus.getResourceStatus(cluster)).isEqualTo(ResourceStatus.Unknown)
        clusterController.setResourceStatus(ResourceStatus.Updated)
        assertThat(FlinkClusterStatus.getResourceStatus(cluster)).isEqualTo(ResourceStatus.Updated)
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return resource status`() {
        assertThat(clusterController.getResourceStatus()).isEqualTo(ResourceStatus.Unknown)
        FlinkClusterStatus.setResourceStatus(cluster, ResourceStatus.Updating)
        assertThat(clusterController.getResourceStatus()).isEqualTo(ResourceStatus.Updating)
    }

    @Test
    fun `should reset action`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isLessThan(timestamp)
        FlinkClusterAnnotations.setRequestedAction(cluster, Action.STOP)
        clusterController.resetAction()
        assertThat(FlinkClusterAnnotations.getRequestedAction(cluster)).isEqualTo(Action.NONE)
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return action`() {
        FlinkClusterAnnotations.setRequestedAction(cluster, Action.START)
        assertThat(clusterController.getAction()).isEqualTo(Action.START)
        FlinkClusterAnnotations.setRequestedAction(cluster, Action.STOP)
        assertThat(clusterController.getAction()).isEqualTo(Action.STOP)
    }

    @Test
    fun `should update delete resources`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isLessThan(timestamp)
        assertThat(FlinkClusterAnnotations.isDeleteResources(cluster)).isEqualTo(false)
        clusterController.setDeleteResources(true)
        assertThat(FlinkClusterAnnotations.isDeleteResources(cluster)).isEqualTo(true)
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return true when resource deleted otherwise false`() {
        FlinkClusterAnnotations.setDeleteResources(cluster, false)
        assertThat(clusterController.isDeleteResources()).isFalse()
        FlinkClusterAnnotations.setDeleteResources(cluster, true)
        assertThat(clusterController.isDeleteResources()).isTrue()
    }

    @Test
    fun `should update without savepoint`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isLessThan(timestamp)
        assertThat(FlinkClusterAnnotations.isWithoutSavepoint(cluster)).isEqualTo(false)
        clusterController.setWithoutSavepoint(true)
        assertThat(FlinkClusterAnnotations.isWithoutSavepoint(cluster)).isEqualTo(true)
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return true when without savepoint otherwise false`() {
        FlinkClusterAnnotations.setWithoutSavepoint(cluster, false)
        assertThat(clusterController.isWithoutSavepoint()).isFalse()
        FlinkClusterAnnotations.setWithoutSavepoint(cluster, true)
        assertThat(clusterController.isWithoutSavepoint()).isTrue()
    }

    @Test
    fun `should update should restart`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isLessThan(timestamp)
        assertThat(FlinkClusterAnnotations.shouldRestart(cluster)).isEqualTo(false)
        clusterController.setShouldRestart(true)
        assertThat(FlinkClusterAnnotations.shouldRestart(cluster)).isEqualTo(true)
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return true when should restart otherwise false`() {
        FlinkClusterAnnotations.setShouldRestart(cluster, false)
        assertThat(clusterController.shouldRestart()).isFalse()
        FlinkClusterAnnotations.setShouldRestart(cluster, true)
        assertThat(clusterController.shouldRestart()).isTrue()
    }

    @Test
    fun `should update cluster health`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isLessThan(timestamp)
        assertThat(FlinkClusterStatus.getClusterHealth(cluster)).isNull()
        clusterController.setClusterHealth("HEALTHY")
        assertThat(FlinkClusterStatus.getClusterHealth(cluster)).isEqualTo("HEALTHY")
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should create service from cluster spec`() {
        cluster.metadata.uid = "123"
        val service = ClusterResourcesDefaultFactory.createService(
            "flink", "flink-operator", "test", cluster.spec
        )
        val result: Result<String?> = Result(status = ResultStatus.OK, output = "servicename")
        given(controller.createService(eq("flink"), eq("test"), eq(service))).thenReturn(result)
        assertThat(clusterController.createService()).isEqualTo(result)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).createService(eq("flink"), eq("test"), eq(service))
    }

    @Test
    fun `should not create jobmanager pods when pods already exists`() {
        val pod = TestFactory.aJobManagerPod(cluster, "1")
        val result: Result<String?> = Result(status = ResultStatus.OK, output = null)
        given(controller.createPod(eq("flink"), eq("test"), eq(pod))).thenReturn(result)
        val expectedResult: Result<Set<String>> = Result(status = ResultStatus.OK, output = setOf())
        assertThat(clusterController.createJobManagerPods(1)).isEqualTo(expectedResult)
    }

    @Test
    fun `should create jobmanager pods from cluster spec when pods don't exist`() {
        cluster.metadata.uid = "123"
        jobmanagerPods.clear()
        val pod = ClusterResourcesDefaultFactory.createJobManagerPod(
            "flink", "flink-operator", "test", cluster.spec
        )
        val result: Result<String?> = Result(status = ResultStatus.OK, output = "podname")
        given(controller.createPod(eq("flink"), eq("test"), eq(pod))).thenReturn(result)
        val expectedResult: Result<Set<String>> = Result(status = ResultStatus.OK, output = setOf("podname"))
        assertThat(clusterController.createJobManagerPods(1)).isEqualTo(expectedResult)
        verify(controller, times(1)).currentTimeMillis()
        verify(controller, times(1)).createPod(eq("flink"), eq("test"), eq(pod))
    }

    @Test
    fun `should not create taskmanager pods from cluster spec when pods already exist`() {
        val pod = TestFactory.aTaskManagerPod(cluster, "1")
        val result: Result<String?> = Result(status = ResultStatus.OK, output = null)
        given(controller.createPod(eq("flink"), eq("test"), eq(pod))).thenReturn(result)
        val expectedResult: Result<Set<String>> = Result(status = ResultStatus.OK, output = setOf())
        assertThat(clusterController.createTaskManagerPods(1)).isEqualTo(expectedResult)
    }

    @Test
    fun `should create taskmanager pods from cluster spec when pods don't exist`() {
        cluster.metadata.uid = "123"
        taskmanagerPods.clear()
        val pod = ClusterResourcesDefaultFactory.createTaskManagerPod(
            "flink", "flink-operator", "test", cluster.spec
        )
        val result: Result<String?> = Result(status = ResultStatus.OK, output = "podname")
        given(controller.createPod(eq("flink"), eq("test"), eq(pod))).thenReturn(result)
        val expectedResult: Result<Set<String>> = Result(status = ResultStatus.OK, output = setOf("podname"))
        assertThat(clusterController.createTaskManagerPods(2)).isEqualTo(expectedResult)
        verify(controller, times(2)).currentTimeMillis()
        verify(controller, times(2)).createPod(eq("flink"), eq("test"), eq(pod))
    }

    @Test
    fun `should return action timestamp`() {
        assertThat(clusterController.getActionTimestamp()).isEqualTo(FlinkClusterAnnotations.getActionTimestamp(cluster))
        FlinkClusterAnnotations.setRequestedAction(cluster, Action.STOP)
        assertThat(clusterController.getActionTimestamp()).isEqualTo(FlinkClusterAnnotations.getActionTimestamp(cluster))
    }

    @Test
    fun `should return status timestamp`() {
        assertThat(clusterController.getStatusTimestamp()).isEqualTo(DateTime(0))
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopped)
        assertThat(clusterController.getStatusTimestamp()).isEqualTo(FlinkClusterStatus.getStatusTimestamp(cluster))
    }

    @Test
    fun `should return true when jobmanager service exists otherwise false`() {
        assertThat(clusterController.doesJobManagerServiceExists()).isTrue()
        // TODO perhaps we can fin a better way to do this
        val newResources = resources.withService(null)
        val newController = ClusterController("flink", "test", 5, controller, newResources, cluster)
        assertThat(newController.doesJobManagerServiceExists()).isFalse()
    }

    @Test
    fun `should return true when jobmanager pods exist otherwise false`() {
        assertThat(clusterController.doesJobManagerPodExists()).isTrue()
        jobmanagerPods.clear()
        assertThat(clusterController.doesJobManagerPodExists()).isFalse()
    }

    @Test
    fun `should return true when taskmanager pods exist otherwise false`() {
        assertThat(clusterController.doesTaskManagerPodsExist()).isTrue()
        taskmanagerPods.clear()
        assertThat(clusterController.doesTaskManagerPodsExist()).isFalse()
    }

    @Test
    fun `should return number of task managers`() {
        cluster.spec.taskManagers = 4
        assertThat(clusterController.getClampedTaskManagers()).isEqualTo(4)
        cluster.spec.taskManagers = 2
        assertThat(clusterController.getClampedTaskManagers()).isEqualTo(2)
    }

    @Test
    fun `should return current number of task managers`() {
        assertThat(clusterController.getCurrentTaskManagers()).isEqualTo(0)
        FlinkClusterStatus.setTaskManagers(cluster, 4)
        assertThat(clusterController.getCurrentTaskManagers()).isEqualTo(4)
    }

    @Test
    fun `should return required number of task managers`() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
        FlinkJobStatus.setJobParallelism(job, 0)
        assertThat(clusterController.getClampedRequiredTaskManagers()).isEqualTo(0)
        FlinkJobStatus.setJobParallelism(job, 2)
        cluster.spec?.taskManager?.taskSlots = 2
        assertThat(clusterController.getClampedRequiredTaskManagers()).isEqualTo(1)
        cluster.spec?.taskManager?.taskSlots = 1
        assertThat(clusterController.getClampedRequiredTaskManagers()).isEqualTo(2)
    }

    @Test
    fun `should return required number of task slots`() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
        FlinkJobStatus.setJobParallelism(job, 0)
        assertThat(clusterController.getRequiredTaskSlots()).isEqualTo(0)
        FlinkJobStatus.setJobParallelism(job, 2)
        assertThat(clusterController.getRequiredTaskSlots()).isEqualTo(2)
    }

    @Test
    fun `should return number of jobmanager replicas`() {
        jobmanagerPods.clear()
        jobmanagerPods.add(TestFactory.aJobManagerPod(cluster,"1"))
        jobmanagerPods.add(TestFactory.aJobManagerPod(cluster,"2"))
        assertThat(clusterController.getJobManagerReplicas()).isEqualTo(2)
    }

    @Test
    fun `should return number of taskmanager replicas`() {
        taskmanagerPods.clear()
        taskmanagerPods.add(TestFactory.aTaskManagerPod(cluster,"1"))
        taskmanagerPods.add(TestFactory.aTaskManagerPod(cluster,"2"))
        taskmanagerPods.add(TestFactory.aTaskManagerPod(cluster,"3"))
        taskmanagerPods.add(TestFactory.aTaskManagerPod(cluster,"4"))
        assertThat(clusterController.getTaskManagerReplicas()).isEqualTo(4)
    }

    @Test
    fun `should return rescale delay`() {
        cluster.spec.supervisor.rescaleDelay = 60
        assertThat(clusterController.getRescaleDelay()).isEqualTo(60)
        cluster.spec.supervisor.rescaleDelay = 90
        assertThat(clusterController.getRescaleDelay()).isEqualTo(90)
    }

    @Test
    fun `should rescale cluster`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isLessThan(timestamp)
        FlinkClusterStatus.setTaskManagers(cluster, 2)
        clusterController.rescaleCluster(4)
        assertThat(FlinkClusterStatus.getTaskManagers(cluster)).isEqualTo(4)
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
        verify(controller, times(1)).updateTaskManagerReplicas(eq("flink"), eq("test"), eq(4))
    }

    @Test
    fun `should not remove taskmanagers when there is an error`() {
        given(controller.getTaskManagerStatus(eq("flink"), eq("test"))).thenReturn(Result(ResultStatus.ERROR, null))
        clusterController.removeUnusedTaskManagers()
        verify(controller, times(1)).getTaskManagerStatus(eq("flink"), eq("test"))
    }

    @Test
    fun `should remove taskmanagers when there are unused taskmanagers`() {
        val pod1 = TestFactory.aTaskManagerPod(cluster, "1")
        val pod2 = TestFactory.aTaskManagerPod(cluster, "2")
        val pod3 = TestFactory.aTaskManagerPod(cluster, "3")
        val pod4 = TestFactory.aTaskManagerPod(cluster, "4")
        taskmanagerPods.clear()
        taskmanagerPods.add(pod1)
        taskmanagerPods.add(pod2)
        taskmanagerPods.add(pod3)
        taskmanagerPods.add(pod4)
        val taskManagersInfo = TaskManagersInfo()
        val taskManagerInfo1 = TaskManagerInfo()
        taskManagerInfo1.id = "1"
        taskManagerInfo1.freeSlots = 0
        taskManagerInfo1.slotsNumber = 4
        taskManagerInfo1.path = "akka.tcp://flink@172.17.0.12:41545/user/taskmanager_0"
        taskManagersInfo.addTaskmanagersItem(taskManagerInfo1)
        val taskManagerInfo2 = TaskManagerInfo()
        taskManagerInfo2.id = "2"
        taskManagerInfo2.freeSlots = 2
        taskManagerInfo2.slotsNumber = 4
        taskManagerInfo2.path = "akka.tcp://flink@172.17.0.13:41545/user/taskmanager_0"
        taskManagersInfo.addTaskmanagersItem(taskManagerInfo2)
        val taskManagerInfo3 = TaskManagerInfo()
        taskManagerInfo3.id = "3"
        taskManagerInfo3.freeSlots = 4
        taskManagerInfo3.slotsNumber = 4
        taskManagerInfo3.path = "akka.tcp://flink@172.17.0.14:41545/user/taskmanager_0"
        taskManagersInfo.addTaskmanagersItem(taskManagerInfo3)
        val taskManagerInfo4 = TaskManagerInfo()
        taskManagerInfo4.id = "4"
        taskManagerInfo4.freeSlots = 4
        taskManagerInfo4.slotsNumber = 4
        taskManagerInfo4.path = "akka.tcp://flink@172.17.0.15:41545/user/taskmanager_0"
        taskManagersInfo.addTaskmanagersItem(taskManagerInfo4)
        pod1.status = V1PodStatus()
        pod2.status = V1PodStatus()
        pod3.status = V1PodStatus()
        pod4.status = V1PodStatus()
        pod1.status?.podIP = "172.17.0.12"
        pod2.status?.podIP = "172.17.0.13"
        pod3.status?.podIP = "172.17.0.14"
        pod4.status?.podIP = "172.17.0.15"
        given(controller.getTaskManagerStatus(eq("flink"), eq("test"))).thenReturn(Result(ResultStatus.OK, taskManagersInfo))
        clusterController.removeUnusedTaskManagers()
        verify(controller, times(2)).currentTimeMillis()
        verify(controller, times(1)).getTaskManagerStatus(eq("flink"), eq("test"))
        verify(controller, times(1)).deletePod(eq("flink"), eq("test"), eq(pod3.metadata?.name ?: ""))
        verify(controller, times(1)).deletePod(eq("flink"), eq("test"), eq(pod4.metadata?.name ?: ""))
    }
}
