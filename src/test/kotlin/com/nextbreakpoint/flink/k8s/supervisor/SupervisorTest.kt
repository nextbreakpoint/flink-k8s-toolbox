package com.nextbreakpoint.flink.k8s.supervisor

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterResources
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager
import com.nextbreakpoint.flink.k8s.supervisor.core.JobResources
import com.nextbreakpoint.flink.k8s.supervisor.core.Task
import com.nextbreakpoint.flink.testing.KotlinMockito.any
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import com.nextbreakpoint.flink.testing.TestFactory
import org.apache.log4j.Logger
import org.assertj.core.api.Assertions.assertThat
import org.joda.time.DateTime
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.spy
import org.mockito.Mockito.times
import org.mockito.Mockito.verify

class SupervisorTest {
    private val clusterSelector = ResourceSelector(name = "test", namespace = "flink", uid = "123")
    private val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
    private val job = TestFactory.aFlinkJob(cluster)
    private val clusterResources = ClusterResources(
        flinkCluster = cluster,
        service = TestFactory.aJobManagerService(cluster),
        jobmanagerPods = setOf(TestFactory.aJobManagerPod(cluster, "1")),
        taskmanagerPods = setOf(TestFactory.aTaskManagerPod(cluster, "1")),
        flinkJobs = mutableMapOf("test" to job)
    )
    private val jobResources = JobResources(
        flinkJob = job,
        bootstrapJob = TestFactory.aBootstrapJob(cluster)
    )
    private val clusterTask = spy(DummyClusterTask(cluster))
    private val clusterTasks = mapOf(
        ClusterStatus.Unknown to clusterTask,
        ClusterStatus.Starting to clusterTask,
        ClusterStatus.Stopping to clusterTask,
        ClusterStatus.Started to clusterTask,
        ClusterStatus.Stopped to clusterTask,
        ClusterStatus.Terminated to clusterTask
    )
    private val jobTask = spy(DummyJobTask(job))
    private val jobTasks = mapOf(
        JobStatus.Unknown to jobTask,
        JobStatus.Starting to jobTask,
        JobStatus.Stopping to jobTask,
        JobStatus.Started to jobTask,
        JobStatus.Stopped to jobTask,
        JobStatus.Terminated to jobTask
    )
    private val cache = mock(Cache::class.java)
    private val controller = mock(Controller::class.java)
    private val supervisor = Supervisor.create(controller = controller, cache = cache, taskTimeout = 60, clusterTasks = clusterTasks, jobTasks = jobTasks)

    @Test
    fun `should update status when reconciling resources`() {
        cluster.metadata.uid = "123"
        job.metadata.uid = "456"
        given(cache.getCachedClusterResources(eq(clusterSelector))).thenReturn(clusterResources)
        given(cache.getCachedJobResources(eq(clusterSelector), eq("test"))).thenReturn(jobResources)
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isEqualTo(DateTime(0))
        supervisor.reconcile(clusterSelector)
        verify(clusterTask, times(1)).execute(any())
        verify(jobTask, times(1)).execute(any())
        assertThat(FlinkClusterStatus.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(FlinkJobStatus.getStatusTimestamp(job)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should match podIP`() {
        val regexp = Regex("akka\\.tcp://flink@([0-9.]+):[0-9]+/user/taskmanager_[0-9]+")
        val match = regexp.matchEntire("akka.tcp://flink@172.17.0.12:41545/user/taskmanager_0")
        val nodeIP = match?.groupValues?.get(1)
        assertThat(nodeIP).isEqualTo("172.17.0.12")
    }

    class DummyClusterTask(val cluster: V2FlinkCluster) : Task<ClusterManager>() {
        override fun execute(manager: ClusterManager) {
            FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Started)
        }
    }

    class DummyJobTask(val job: V1FlinkJob) : Task<JobManager>() {
        override fun execute(manager: JobManager) {
            FlinkJobStatus.setSupervisorStatus(job, JobStatus.Started)
        }
    }
}
