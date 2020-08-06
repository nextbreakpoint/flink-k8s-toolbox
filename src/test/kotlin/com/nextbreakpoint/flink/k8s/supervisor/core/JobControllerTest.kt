package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.SavepointOptions
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.common.FlinkJobAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.factory.BootstrapResourcesDefaultFactory
import com.nextbreakpoint.flink.testing.KotlinMockito.any
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import com.nextbreakpoint.flink.testing.TestFactory
import org.apache.log4j.Logger
import org.assertj.core.api.Assertions.assertThat
import org.joda.time.DateTime
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify

class JobControllerTest {
    private val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
    private val job = TestFactory.aFlinkJob(cluster)
    private val bootstrapJob = TestFactory.aBootstrapJob(cluster)
    private val flinkJobs = mutableMapOf("test" to job)
    private val clusterSelector = ResourceSelector(name = "test", namespace = "flink", uid = "123")
    private val jobSelector = ResourceSelector(name = "test-test", namespace = "flink", uid = "456")
    private val savepointRequest = SavepointRequest(jobId = "1", triggerId = "100")
    private val savepointOptions = SavepointOptions(targetPath = "file:///tmp")
    private val clusterResources = ClusterResources(
        flinkCluster = cluster,
        service = TestFactory.aJobManagerService(cluster),
        jobmanagerPods = setOf(TestFactory.aJobManagerPod(cluster, "1")),
        taskmanagerPods = setOf(TestFactory.aTaskManagerPod(cluster, "1")),
        flinkJobs = flinkJobs
    )
    private val jobResources = JobResources(
        flinkJob = job,
        bootstrapJob = bootstrapJob
    )
    private val logger = mock(Logger::class.java)
    private val controller = mock(Controller::class.java)
    private val jobController = JobController(clusterSelector, jobSelector, cluster, job, clusterResources, jobResources, controller)

    @BeforeEach
    fun setup() {
        given(controller.isDryRun()).thenReturn(false)
    }

    @AfterEach
    fun verifyInteractions() {
        Mockito.verifyNoMoreInteractions(controller)
    }

    @Test
    fun `should return time passed since last update`() {
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 5000)
        assertThat(jobController.timeSinceLastUpdateInSeconds()).isGreaterThanOrEqualTo(5)
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 10000)
        assertThat(jobController.timeSinceLastUpdateInSeconds()).isGreaterThanOrEqualTo(10)
        verify(controller, times(2)).currentTimeMillis()
    }

    @Test
    fun `should return time passed since last savepoint request`() {
        FlinkJobStatus.setSavepointRequest(job, savepointRequest)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 5000)
        assertThat(jobController.timeSinceLastSavepointRequestInSeconds()).isGreaterThanOrEqualTo(5)
        FlinkJobStatus.setSavepointRequest(job, savepointRequest)
        given(controller.currentTimeMillis()).thenReturn(System.currentTimeMillis() + 10000)
        assertThat(jobController.timeSinceLastSavepointRequestInSeconds()).isGreaterThanOrEqualTo(10)
        verify(controller, times(2)).currentTimeMillis()
    }

    @Test
    fun `should trigger savepoint`() {
        val result: Result<SavepointRequest?> = Result(status = ResultStatus.OK, output = savepointRequest)
        given(controller.triggerSavepoint(eq(clusterSelector), eq(savepointOptions), any())).thenReturn(result)
        assertThat(jobController.triggerSavepoint(savepointOptions)).isEqualTo(result)
        verify(controller, times(1)).triggerSavepoint(eq(clusterSelector), eq(savepointOptions), any())
    }

    @Test
    fun `should query savepoint`() {
        val result: Result<String?> = Result(status = ResultStatus.OK, output = "file:///tmp/1")
        given(controller.querySavepoint(eq(clusterSelector), eq(savepointRequest), any())).thenReturn(result)
        assertThat(jobController.querySavepoint(savepointRequest)).isEqualTo(result)
        verify(controller, times(1)).querySavepoint(eq(clusterSelector), eq(savepointRequest), any())
    }

    @Test
    fun `should create bootstrap job`() {
        val result: Result<String?> = Result(status = ResultStatus.OK, output = "jobname")
        given(controller.createBootstrapJob(eq(clusterSelector), eq(bootstrapJob))).thenReturn(result)
        assertThat(jobController.createBootstrapJob(bootstrapJob)).isEqualTo(result)
        verify(controller, times(1)).createBootstrapJob(eq(clusterSelector), eq(bootstrapJob))
    }

    @Test
    fun `should delete bootstrap job`() {
        val result: Result<Void?> = Result(status = ResultStatus.OK, output = null)
        given(controller.deleteBootstrapJob(eq(clusterSelector), eq("test"))).thenReturn(result)
        assertThat(jobController.deleteBootstrapJob()).isEqualTo(result)
        verify(controller, times(1)).deleteBootstrapJob(eq(clusterSelector), eq("test"))
    }

    @Test
    fun `should stop job`() {
        val result: Result<Void?> = Result(status = ResultStatus.OK, output = null)
        given(controller.stopJob(eq(clusterSelector), any())).thenReturn(result)
        assertThat(jobController.stopJob()).isEqualTo(result)
        verify(controller, times(1)).stopJob(eq(clusterSelector), any())
    }

    @Test
    fun `should cancel job`() {
        val result: Result<SavepointRequest?> = Result(status = ResultStatus.OK, output = savepointRequest)
        given(controller.cancelJob(eq(clusterSelector), eq(savepointOptions), any())).thenReturn(result)
        assertThat(jobController.cancelJob(savepointOptions)).isEqualTo(result)
        verify(controller, times(1)).cancelJob(eq(clusterSelector), eq(savepointOptions), any())
    }

    @Test
    fun `should return true when job status is canceled otherwise false`() {
        FlinkJobStatus.setJobStatus(job, "CANCELED")
        assertThat(jobController.isJobCancelled()).isTrue()
        FlinkJobStatus.setJobStatus(job, "")
        assertThat(jobController.isJobCancelled()).isFalse()
    }

    @Test
    fun `should return true when job status is finished otherwise false`() {
        FlinkJobStatus.setJobStatus(job, "FINISHED")
        assertThat(jobController.isJobFinished()).isTrue()
        FlinkJobStatus.setJobStatus(job, "")
        assertThat(jobController.isJobFinished()).isFalse()
    }

    @Test
    fun `should return true when job status is failed otherwise false`() {
        FlinkJobStatus.setJobStatus(job, "FAILED")
        assertThat(jobController.isJobFailed()).isTrue()
        FlinkJobStatus.setJobStatus(job, "")
        assertThat(jobController.isJobFailed()).isFalse()
    }

    @Test
    fun `should verify if cluster is ready`() {
        val result: Result<Boolean> = Result(status = ResultStatus.OK, output = true)
        given(controller.isClusterReady(eq(clusterSelector), eq(2))).thenReturn(result)
        assertThat(jobController.isClusterReady(2)).isEqualTo(result)
        verify(controller, times(1)).isClusterReady(eq(clusterSelector), eq(2))
    }

    @Test
    fun `should verify if cluster is healthy`() {
        val result: Result<Boolean> = Result(status = ResultStatus.OK, output = true)
        given(controller.isClusterHealthy(eq(clusterSelector))).thenReturn(result)
        assertThat(jobController.isClusterHealthy()).isEqualTo(result)
        verify(controller, times(1)).isClusterHealthy(eq(clusterSelector))
    }

    @Test
    fun `should return true when cluster is stopped otherwise false`() {
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Unknown)
        assertThat(jobController.isClusterStopped()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Starting)
        assertThat(jobController.isClusterStopped()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Started)
        assertThat(jobController.isClusterStopped()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopping)
        assertThat(jobController.isClusterStopped()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopped)
        assertThat(jobController.isClusterStopped()).isTrue()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Terminated)
        assertThat(jobController.isClusterStopped()).isFalse()
    }

    @Test
    fun `should return true when cluster is stopping otherwise false`() {
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Unknown)
        assertThat(jobController.isClusterStopping()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Starting)
        assertThat(jobController.isClusterStopping()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Started)
        assertThat(jobController.isClusterStopping()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopping)
        assertThat(jobController.isClusterStopping()).isTrue()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopped)
        assertThat(jobController.isClusterStopping()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Terminated)
        assertThat(jobController.isClusterStopping()).isFalse()
    }

    @Test
    fun `should return true when cluster is started otherwise false`() {
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Unknown)
        assertThat(jobController.isClusterStarted()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Starting)
        assertThat(jobController.isClusterStarted()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Started)
        assertThat(jobController.isClusterStarted()).isTrue()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopping)
        assertThat(jobController.isClusterStarted()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopped)
        assertThat(jobController.isClusterStarted()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Terminated)
        assertThat(jobController.isClusterStarted()).isFalse()
    }

    @Test
    fun `should return true when cluster is starting otherwise false`() {
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Unknown)
        assertThat(jobController.isClusterStarting()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Starting)
        assertThat(jobController.isClusterStarting()).isTrue()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Started)
        assertThat(jobController.isClusterStarting()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopping)
        assertThat(jobController.isClusterStarting()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopped)
        assertThat(jobController.isClusterStarting()).isFalse()
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Terminated)
        assertThat(jobController.isClusterStarting()).isFalse()
    }

    @Test
    fun `should return true when cluster is updated otherwise false`() {
        FlinkClusterStatus.setResourceStatus(cluster, ResourceStatus.Unknown)
        assertThat(jobController.isClusterUpdated()).isFalse()
        FlinkClusterStatus.setResourceStatus(cluster, ResourceStatus.Updating)
        assertThat(jobController.isClusterUpdated()).isFalse()
        FlinkClusterStatus.setResourceStatus(cluster, ResourceStatus.Updated)
        assertThat(jobController.isClusterUpdated()).isTrue()
    }

    @Test
    fun `should refresh status`() {
        FlinkJobStatus.setSavepointMode(job, "Manual")
        FlinkJobStatus.setRestartPolicy(job, "Always")

        job.metadata.finalizers = listOf("finalizer.nextbreakpoint.com")

        val timestamp = System.currentTimeMillis()

        jobController.refreshStatus(logger, DateTime(timestamp), DateTime(timestamp), false)

        assertThat(FlinkJobStatus.getSavepointMode(job)).isEqualTo("Automatic")
        assertThat(FlinkJobStatus.getRestartPolicy(job)).isEqualTo("Never")

        verify(controller, times(1)).updateStatus(eq(jobSelector), eq(job))
        verify(controller, times(1)).updateFinalizers(eq(jobSelector), eq(job))
        verify(controller, times(1)).updateAnnotations(eq(jobSelector), eq(job))
    }

    @Test
    fun `should return true when resource has been deleted`() {
        assertThat(jobController.hasBeenDeleted()).isEqualTo(false)
        job.metadata.deletionTimestamp = DateTime(System.currentTimeMillis())
        assertThat(jobController.hasBeenDeleted()).isEqualTo(true)
    }

    @Test
    fun `should return true when resource has finalizer`() {
        assertThat(jobController.hasFinalizer()).isEqualTo(false)
        job.metadata.finalizers = listOf("finalizer.nextbreakpoint.com")
        assertThat(jobController.hasFinalizer()).isEqualTo(true)
    }

    @Test
    fun `should add finalizer`() {
        assertThat(jobController.hasFinalizer()).isEqualTo(false)
        jobController.addFinalizer()
        assertThat(jobController.hasFinalizer()).isEqualTo(true)
    }

    @Test
    fun `should remove finalizer`() {
        job.metadata.finalizers = listOf("finalizer.nextbreakpoint.com")
        assertThat(jobController.hasFinalizer()).isEqualTo(true)
        jobController.removeFinalizer()
        assertThat(jobController.hasFinalizer()).isEqualTo(false)
    }

    @Test
    fun `should initialize status`() {
        assertThat(FlinkJobStatus.getBootstrap(job)).isNull()
        assertThat(FlinkJobStatus.getJobParallelism(job)).isEqualTo(1)
        assertThat(FlinkJobStatus.getSavepointPath(job)).isNull()
        assertThat(FlinkJobStatus.getLabelSelector(job)).isNull()
        assertThat(FlinkJobStatus.getSavepointMode(job)).isNull()
        assertThat(FlinkJobStatus.getRestartPolicy(job)).isNull()
        job.spec?.savepoint?.savepointPath = "file:///tmp/1"
        jobController.initializeStatus()
        assertThat(FlinkJobStatus.getBootstrap(job)).isNotNull()
        assertThat(FlinkJobStatus.getJobParallelism(job)).isEqualTo(2)
        assertThat(FlinkJobStatus.getSavepointPath(job)).isEqualTo("file:///tmp/1")
        assertThat(FlinkJobStatus.getLabelSelector(job)).isNotEmpty()
        assertThat(FlinkJobStatus.getSavepointMode(job)).isEqualTo("Automatic")
        assertThat(FlinkJobStatus.getRestartPolicy(job)).isEqualTo("Never")
    }

    @Test
    fun `should initialize annotations`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isEqualTo(DateTime(0))
        FlinkJobAnnotations.setManualAction(job, ManualAction.STOP)
        FlinkJobAnnotations.setDeleteResources(job, true)
        FlinkJobAnnotations.setWithoutSavepoint(job, true)
        jobController.initializeAnnotations()
        assertThat(FlinkJobAnnotations.getManualAction(job)).isEqualTo(ManualAction.NONE)
        assertThat(FlinkJobAnnotations.isDeleteResources(job)).isFalse()
        assertThat(FlinkJobAnnotations.isWithoutSavepoint(job)).isFalse()
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update digests`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobStatus.getBootstrapDigest(job)).isNull()
        jobController.updateDigests()
        assertThat(FlinkJobStatus.getBootstrapDigest(job)).isNotNull()
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update status`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobStatus.getBootstrap(job)).isNull()
        job.status?.jobParallelism = 4
        job.status?.jobStatus = "test"
        jobController.updateStatus()
        assertThat(FlinkJobStatus.getBootstrap(job)).isNotNull()
        assertThat(FlinkJobStatus.getJobParallelism(job)).isEqualTo(2)
        assertThat(FlinkJobStatus.getJobStatus(job)).isEqualTo("")
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return one change when bootstrap digest changed`() {
        jobController.updateDigests()
        FlinkJobStatus.setBootstrapDigest(job, "0")
        val changes = jobController.computeChanges()
        assertThat(changes).containsExactly("BOOTSTRAP")
    }

    @Test
    fun `should return no changes when digests didn't change`() {
        jobController.updateDigests()
        val changes = jobController.computeChanges()
        assertThat(changes).isEmpty()
    }

    @Test
    fun `should update supervisor status`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobStatus.getSupervisorStatus(job)).isEqualTo(JobStatus.Unknown)
        jobController.setSupervisorStatus(JobStatus.Started)
        assertThat(jobController.getSupervisorStatus()).isEqualTo(JobStatus.Started)
        assertThat(FlinkJobStatus.getSupervisorStatus(job)).isEqualTo(JobStatus.Started)
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return supervisor status`() {
        assertThat(jobController.getSupervisorStatus()).isEqualTo(JobStatus.Unknown)
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
        assertThat(jobController.getSupervisorStatus()).isEqualTo(JobStatus.Started)
    }

    @Test
    fun `should update resource status`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobStatus.getResourceStatus(job)).isEqualTo(ResourceStatus.Unknown)
        jobController.setResourceStatus(ResourceStatus.Updated)
        assertThat(FlinkJobStatus.getResourceStatus(job)).isEqualTo(ResourceStatus.Updated)
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return resource status`() {
        assertThat(jobController.getResourceStatus()).isEqualTo(ResourceStatus.Unknown)
        FlinkJobStatus.setResourceStatus(job, ResourceStatus.Updating)
        assertThat(jobController.getResourceStatus()).isEqualTo(ResourceStatus.Updating)
    }

    @Test
    fun `should reset action`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isEqualTo(DateTime(0))
        FlinkJobAnnotations.setManualAction(job, ManualAction.STOP)
        jobController.resetAction()
        assertThat(FlinkJobAnnotations.getManualAction(job)).isEqualTo(ManualAction.NONE)
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return action`() {
        FlinkJobAnnotations.setManualAction(job, ManualAction.START)
        assertThat(jobController.getAction()).isEqualTo(ManualAction.START)
        FlinkJobAnnotations.setManualAction(job, ManualAction.STOP)
        assertThat(jobController.getAction()).isEqualTo(ManualAction.STOP)
    }

    @Test
    fun `should update delete resources`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobAnnotations.isDeleteResources(job)).isEqualTo(false)
        jobController.setDeleteResources(true)
        assertThat(FlinkJobAnnotations.isDeleteResources(job)).isEqualTo(true)
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return true when resource deleted otherwise false`() {
        FlinkJobAnnotations.setDeleteResources(job, false)
        assertThat(jobController.isDeleteResources()).isFalse()
        FlinkJobAnnotations.setDeleteResources(job, true)
        assertThat(jobController.isDeleteResources()).isTrue()
    }

    @Test
    fun `should update without savepoint`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobAnnotations.isWithoutSavepoint(job)).isEqualTo(false)
        jobController.setWithoutSavepoint(true)
        assertThat(FlinkJobAnnotations.isWithoutSavepoint(job)).isEqualTo(true)
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return true when without savepoint otherwise false`() {
        FlinkJobAnnotations.setWithoutSavepoint(job, false)
        assertThat(jobController.isWithoutSavepoint()).isFalse()
        FlinkJobAnnotations.setWithoutSavepoint(job, true)
        assertThat(jobController.isWithoutSavepoint()).isTrue()
    }

    @Test
    fun `should update should restart`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobAnnotations.shouldRestart(job)).isEqualTo(false)
        jobController.setShouldRestart(true)
        assertThat(FlinkJobAnnotations.shouldRestart(job)).isEqualTo(true)
        assertThat(FlinkJobAnnotations.getActionTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return true when should restart otherwise false`() {
        FlinkJobAnnotations.setShouldRestart(job, false)
        assertThat(jobController.shouldRestart()).isFalse()
        FlinkJobAnnotations.setShouldRestart(job, true)
        assertThat(jobController.shouldRestart()).isTrue()
    }

    @Test
    fun `should update savepoint request`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        val savepointRequest = savepointRequest
        assertThat(FlinkJobStatus.getSavepointRequest(job)).isNull()
        jobController.setSavepointRequest(savepointRequest)
        assertThat(FlinkJobStatus.getSavepointRequest(job)).isEqualTo(savepointRequest)
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return savepoint request`() {
        val savepointRequest = savepointRequest
        assertThat(jobController.getSavepointRequest()).isNull()
        FlinkJobStatus.setSavepointRequest(job, savepointRequest)
        assertThat(FlinkJobStatus.getSavepointRequest(job)).isEqualTo(savepointRequest)
    }

    @Test
    fun `should reset savepoint request`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        val savepointRequest = savepointRequest
        FlinkJobStatus.setSavepointRequest(job, savepointRequest)
        jobController.resetSavepointRequest()
        assertThat(FlinkJobStatus.getSavepointRequest(job)).isNull()
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update savepoint path`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobStatus.getSavepointPath(job)).isNull()
        jobController.setSavepointPath("file:///tmp/1")
        assertThat(FlinkJobStatus.getSavepointPath(job)).isEqualTo("file:///tmp/1")
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update cluster name`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobStatus.getClusterName(job)).isNull()
        jobController.setClusterName("name")
        assertThat(FlinkJobStatus.getClusterName(job)).isEqualTo("name")
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update cluster health`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobStatus.getClusterHealth(job)).isNull()
        jobController.setClusterHealth("HEALTHY")
        assertThat(FlinkJobStatus.getClusterHealth(job)).isEqualTo("HEALTHY")
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update job status`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        assertThat(FlinkJobStatus.getJobStatus(job)).isNull()
        jobController.setJobStatus("RUNNING")
        assertThat(FlinkJobStatus.getJobStatus(job)).isEqualTo("RUNNING")
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should return job restart policy`() {
        assertThat(jobController.getRestartPolicy()).isNull()
        FlinkJobStatus.setRestartPolicy(job, "Never")
        assertThat(jobController.getRestartPolicy()).isEqualTo("Never")
    }

    @Test
    fun `should return savepoint mode`() {
        FlinkJobStatus.setSavepointMode(job, "Manual")
        assertThat(jobController.getSavepointMode()).isEqualTo("Manual")
    }

    @Test
    fun `should return savepoint interval`() {
        assertThat(jobController.getSavepointInterval()).isEqualTo(60)
        job.spec?.savepoint?.savepointInterval = 200
        assertThat(jobController.getSavepointInterval()).isEqualTo(200)
    }

    @Test
    fun `should return savepoint options`() {
        job.spec?.savepoint?.savepointTargetPath = "file:///tmp"
        val savepointOptions = savepointOptions
        assertThat(jobController.getSavepointOptions()).isEqualTo(savepointOptions)
    }

    @Test
    fun `should return action timestamp`() {
        assertThat(jobController.getActionTimestamp()).isEqualTo(FlinkJobAnnotations.getActionTimestamp(job))
        FlinkJobAnnotations.setManualAction(job, ManualAction.STOP)
        assertThat(jobController.getActionTimestamp()).isEqualTo(FlinkJobAnnotations.getActionTimestamp(job))
    }

    @Test
    fun `should return status timestamp`() {
        assertThat(jobController.getStatusTimestamp()).isEqualTo(FlinkJobStatus.getStatusTimestamp(job))
        FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
        assertThat(jobController.getStatusTimestamp()).isEqualTo(FlinkJobStatus.getStatusTimestamp(job))
    }

    @Test
    fun `should return true when bootstrap job exist otherwise false`() {
        assertThat(jobController.doesBootstrapJobExists()).isTrue()
        // TODO perhaps we can fin a better way to do this
        val newResources = jobResources.withBootstrapJob(null)
        val newController = JobController(clusterSelector, jobSelector, cluster, job, clusterResources, newResources, controller)
        assertThat(newController.doesBootstrapJobExists()).isFalse()
    }

    @Test
    fun `should create bootstrap job from job spec`() {
        FlinkJobStatus.setBootstrap(job, job.spec.bootstrap)
        FlinkJobStatus.setSavepointPath(job, "")
        FlinkJobStatus.setJobParallelism(job, 2)
        val job = BootstrapResourcesDefaultFactory.createBootstrapJob(
            clusterSelector, jobSelector, "flink-operator", "test", job.spec.bootstrap, null, 2, false
        )
        val result: Result<String?> = Result(status = ResultStatus.OK, output = job.metadata?.name ?: "")
        given(controller.createBootstrapJob(eq(clusterSelector), eq(job))).thenReturn(result)
        assertThat(jobController.createBootstrapJob()).isEqualTo(result)
        verify(controller, times(1)).isDryRun()
        verify(controller, times(1)).createBootstrapJob(eq(clusterSelector), eq(job))
    }

    @Test
    fun `should return true when job has id`() {
        FlinkJobStatus.setJobStatus(job, "RUNNING")
        assertThat(jobController.hasJobId()).isFalse()
        FlinkJobStatus.setJobId(job, "123")
        assertThat(jobController.hasJobId()).isTrue()
    }

    @Test
    fun `should remove job id and job status`() {
        FlinkJobStatus.setJobStatus(job, "RUNNING")
        assertThat(jobController.hasJobId()).isFalse()
        FlinkJobStatus.setJobId(job, "123")
        assertThat(jobController.hasJobId()).isTrue()
        assertThat(FlinkJobStatus.getJobStatus(job)).isNotEmpty()
        jobController.resetJob()
        assertThat(jobController.hasJobId()).isFalse()
        assertThat(FlinkJobStatus.getJobStatus(job)).isEmpty()
    }

    @Test
    fun `should return required task slots`() {
        FlinkJobStatus.setJobParallelism(job, 0)
        assertThat(jobController.getRequiredTaskSlots()).isEqualTo(0)
        FlinkJobStatus.setJobParallelism(job, 2)
        assertThat(jobController.getRequiredTaskSlots()).isEqualTo(2)
        FlinkJobStatus.setJobParallelism(job, 1)
        assertThat(jobController.getRequiredTaskSlots()).isEqualTo(1)
    }

    @Test
    fun `should return job parallelism`() {
        job.spec.jobParallelism = 0
        assertThat(jobController.getJobParallelism()).isEqualTo(0)
        job.spec.jobParallelism = 2
        assertThat(jobController.getJobParallelism()).isEqualTo(2)
    }

    @Test
    fun `should update current job parallelism`() {
        jobController.setCurrentJobParallelism(4)
        assertThat(jobController.getCurrentJobParallelism()).isEqualTo(4)
        jobController.setCurrentJobParallelism(8)
        assertThat(jobController.getCurrentJobParallelism()).isEqualTo(8)
    }

    @Test
    fun `should return current job parallelism`() {
        FlinkJobStatus.setJobParallelism(job, 0)
        assertThat(jobController.getCurrentJobParallelism()).isEqualTo(0)
        FlinkJobStatus.setJobParallelism(job, 2)
        assertThat(jobController.getCurrentJobParallelism()).isEqualTo(2)
    }

    @Test
    fun `should return cluster job status`() {
        FlinkJobStatus.setJobId(job, "123")
        val result: Result<String?> = Result(status = ResultStatus.OK, output = "RUNNING")
        given(controller.getClusterJobStatus(eq(clusterSelector), eq("123"))).thenReturn(result)
        assertThat(jobController.getClusterJobStatus()).isEqualTo(result)
        verify(controller, times(1)).getClusterJobStatus(eq(clusterSelector), eq("123"))
    }
}
