package com.nextbreakpoint.flink.integration.cases

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.integration.IntegrationSetup
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.fail

@Tag("IntegrationTest")
class StartAndStopTest : IntegrationSetup() {
    companion object {
        @BeforeAll
        @JvmStatic
        fun setup() {
            IntegrationSetup.setup()
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            IntegrationSetup.teardown()
        }
    }

    @AfterEach
    fun printInfo() {
        printSupervisorLogs()
        printOperatorLogs()
        describeResources()
    }

    @Test
    fun `should start and stop clusters`() {
        println("Should start the cluster and its jobs when the cluster is created...")
        deleteCluster(namespace = namespace, path = "integration/cluster-2.yaml")
        awaitUntilAsserted(timeout = 120) {
            assertThat(jobExists(namespace = namespace, jobName = "cluster-2-job-1")).isFalse()
            assertThat(jobExists(namespace = namespace, jobName = "cluster-2-job-2")).isFalse()
        }
        awaitUntilAsserted(timeout = 300) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-2")).isFalse()
        }
        createCluster(namespace = namespace, path = "integration/cluster-2.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-2")).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-2", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updated)).isTrue()
        }
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-1", status = JobStatus.Started)).isTrue()
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-2", status = JobStatus.Started)).isTrue()
        }
        awaitUntilAsserted(timeout = 30) {
            assertThat(getTaskManagers(namespace = namespace, clusterName = "cluster-2")).isEqualTo(2)
        }

        println("Should create savepoints and stop all jobs when the cluster is stopped requesting savepoint...")
        val stopOptions = StopOptions(withoutSavepoint = false)
        stopCluster(clusterName = "cluster-2", options = stopOptions, port = port)
        awaitUntilAsserted(timeout = 120) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-1", status = JobStatus.Stopped)).isTrue()
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-2", status = JobStatus.Stopped)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-2", status = ClusterStatus.Stopped)).isTrue()
        }
        awaitUntilAsserted(timeout = 120) {
            assertThat(getTaskManagers(namespace = namespace, clusterName = "cluster-2")).isEqualTo(0)
        }
        val savepointPath1 = getSavepointPath(namespace = namespace, jobName = "cluster-2-job-1")
        val savepointPath2 = getSavepointPath(namespace = namespace, jobName = "cluster-2-job-2")
        assertThat(savepointPath1).isNotEmpty()
        assertThat(savepointPath2).isNotEmpty()

        println("Should restart the jobs from the savepoints when the cluster is restarted requiring savepoint and job restart policy is always...")
        if (updateCluster(namespace = namespace, clusterName = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/1/spec/savepoint/restartPolicy\",\"value\":\"Always\"}]") != 0) {
            fail("Can't update cluster")
        }
        if (updateJob(namespace = namespace, jobName = "cluster-2-job-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/savepoint/restartPolicy\",\"value\":\"Always\"}]") != 0) {
            fail("Can't update job")
        }
        val startOptions = StartOptions(withoutSavepoint = false)
        startCluster(clusterName = "cluster-2", options = startOptions, port = port)
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-2", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updated)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-2", status = JobStatus.Started)).isTrue()
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-1", status = JobStatus.Stopped)).isTrue()
        }
        awaitUntilAsserted(timeout = 30) {
            assertThat(getTaskManagers(namespace = namespace, clusterName = "cluster-2")).isEqualTo(1)
        }

        println("Should stop all jobs but not create savepoints when the cluster is stopped without requesting savepoint...")
        val stopWithoutSavepointOptions = StopOptions(withoutSavepoint = true)
        stopCluster(clusterName = "cluster-2", options = stopWithoutSavepointOptions, port = port)
        awaitUntilAsserted(timeout = 120) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-1", status = JobStatus.Stopped)).isTrue()
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-2", status = JobStatus.Stopped)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-2", status = ClusterStatus.Stopped)).isTrue()
        }
        awaitUntilAsserted(timeout = 120) {
            assertThat(getTaskManagers(namespace = namespace, clusterName = "cluster-2")).isEqualTo(0)
        }
        val savepointPath3 = getSavepointPath(namespace = namespace, jobName = "cluster-2-job-1")
        val savepointPath4 = getSavepointPath(namespace = namespace, jobName = "cluster-2-job-2")
        assertThat(savepointPath3).isEqualTo(savepointPath1)
        assertThat(savepointPath4).isEqualTo(savepointPath2)

        println("Should restart the jobs but ignore the savepoints when the cluster is restarted without requiring savepoint and job restart policy is always...")
        val startWithoutSavepointOptions = StartOptions(withoutSavepoint = true)
        if (updateCluster(namespace = namespace, clusterName = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/savepoint/restartPolicy\",\"value\":\"Always\"}]") != 0) {
            fail("Can't update cluster")
        }
        if (updateJob(namespace = namespace, jobName = "cluster-2-job-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/savepoint/restartPolicy\",\"value\":\"Always\"}]") != 0) {
            fail("Can't update job")
        }
        startCluster(clusterName = "cluster-2", options = startWithoutSavepointOptions, port = port)
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-2", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updated)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-1", status = JobStatus.Started)).isTrue()
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-2-job-2", status = JobStatus.Started)).isTrue()
        }
        awaitUntilAsserted(timeout = 30) {
            assertThat(getTaskManagers(namespace = namespace, clusterName = "cluster-2")).isEqualTo(2)
        }
        val savepointPath5 = getSavepointPath(namespace = namespace, jobName = "cluster-2-job-1")
        val savepointPath6 = getSavepointPath(namespace = namespace, jobName = "cluster-2-job-2")
        assertThat(savepointPath5).isBlank()
        assertThat(savepointPath6).isBlank()
    }

    @Test
    fun `should start and stop jobs`() {
        println("Should start the cluster and its jobs when the cluster is created...")
        deleteCluster(namespace = namespace, path = "integration/cluster-1.yaml")
        awaitUntilAsserted(timeout = 120) {
            assertThat(jobExists(namespace = namespace, jobName = "cluster-1-job-0")).isFalse()
        }
        awaitUntilAsserted(timeout = 300) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-1")).isFalse()
        }
        createCluster(namespace = namespace, path = "integration/cluster-1.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-1")).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-1", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-1", status = ResourceStatus.Updated)).isTrue()
        }
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
        }

        println("Should create a savepoint and stop the job...")
        val stopOptions = StopOptions(withoutSavepoint = false)
        stopJob(clusterName = "cluster-1", jobName = "job-0", options = stopOptions, port = port)
        awaitUntilAsserted(timeout = 120) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Stopped)).isTrue()
        }
        val savepointPath1 = getSavepointPath(namespace = namespace, jobName = "cluster-1-job-0")
        assertThat(savepointPath1).isNotBlank()

        println("Should restart the job from the savepoint...")
        val startOptions = StartOptions(withoutSavepoint = false)
        startJob(clusterName = "cluster-1", jobName = "job-0", options = startOptions, port = port)
        awaitUntilAsserted(timeout = 120) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
        }
        val savepointPath2 = getSavepointPath(namespace = namespace, jobName = "cluster-1-job-0")
        assertThat(savepointPath2).isEqualTo(savepointPath1)

        println("Should stop the job but not create a savepoint...")
        val stopWithoutSavepointOptions = StopOptions(withoutSavepoint = true)
        stopJob(clusterName = "cluster-1", jobName = "job-0", options = stopWithoutSavepointOptions, port = port)
        awaitUntilAsserted(timeout = 120) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Stopped)).isTrue()
        }
        val savepointPath3 = getSavepointPath(namespace = namespace, jobName = "cluster-1-job-0")
        assertThat(savepointPath3).isEqualTo(savepointPath1)

        println("Should restart the job but ignore the savepoint...")
        val startWithoutSavepointOptions = StartOptions(withoutSavepoint = true)
        startJob(clusterName = "cluster-1", jobName = "job-0", options = startWithoutSavepointOptions, port = port)
        awaitUntilAsserted(timeout = 120) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
        }
        val savepointPath4 = getSavepointPath(namespace = namespace, jobName = "cluster-1-job-0")
        assertThat(savepointPath4).isBlank()
    }

    @Test
    fun `should stop job when job fails`() {
        println("Should start the cluster and its jobs when the cluster is created...")
        deleteCluster(namespace = namespace, path = "integration/cluster-1.yaml")
        awaitUntilAsserted(timeout = 120) {
            assertThat(jobExists(namespace = namespace, jobName = "cluster-1-job-0")).isFalse()
        }
        awaitUntilAsserted(timeout = 300) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-1")).isFalse()
        }
        createCluster(namespace = namespace, path = "integration/cluster-1.yaml")
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(namespace = namespace, clusterName = "cluster-1")).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, clusterName = "cluster-1", status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-1", status = ResourceStatus.Updated)).isTrue()
        }
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
        }

        println("Should stop job when specification changed and job failed...")
        if (updateCluster(namespace = namespace, clusterName = "cluster-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/savepoint/restartPolicy\",\"value\":\"Always\"}]") != 0) {
            fail("Can't update cluster")
        }
        if (updateJob(namespace = namespace, jobName = "cluster-1-job-0", patch = "[{\"op\":\"replace\",\"path\":\"/spec/savepoint/restartPolicy\",\"value\":\"Always\"}]") != 0) {
            fail("Can't update job")
        }
        if (updateCluster(namespace = namespace, clusterName = "cluster-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/bootstrap/image\",\"value\":\"integration/wrongimage\"}]") != 0) {
            fail("Can't update cluster")
        }
        if (updateJob(namespace = namespace, jobName = "cluster-1-job-0", patch = "[{\"op\":\"replace\",\"path\":\"/spec/bootstrap/image\",\"value\":\"integration/wrongimage\"}]") != 0) {
            fail("Can't update job")
        }
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Stopping)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Starting)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Stopped)).isTrue()
        }

        println("Should restart job when specification changed...")
        if (updateCluster(namespace = namespace, clusterName = "cluster-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/bootstrap/image\",\"value\":\"integration/flink-jobs:1\"}]") != 0) {
            fail("Can't update cluster")
        }
        if (updateJob(namespace = namespace, jobName = "cluster-1-job-0", patch = "[{\"op\":\"replace\",\"path\":\"/spec/bootstrap/image\",\"value\":\"integration/flink-jobs:1\"}]") != 0) {
            fail("Can't update job")
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Starting)).isTrue()
        }
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
        }

        println("Should stop job when specification changed and job failed...")
        if (updateCluster(namespace = namespace, clusterName = "cluster-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/bootstrap/className\",\"value\":\"wrongclassname\"}]") != 0) {
            fail("Can't update cluster")
        }
        if (updateJob(namespace = namespace, jobName = "cluster-1-job-0", patch = "[{\"op\":\"replace\",\"path\":\"/spec/bootstrap/className\",\"value\":\"wrongclassname\"}]") != 0) {
            fail("Can't update job")
        }
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Stopping)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Starting)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Stopped)).isTrue()
        }

        println("Should restart job when specification changed...")
        if (updateCluster(namespace = namespace, clusterName = "cluster-1", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/bootstrap/className\",\"value\":\"com.nextbreakpoint.flink.jobs.stream.TestJob\"}]") != 0) {
            fail("Can't update cluster")
        }
        if (updateJob(namespace = namespace, jobName = "cluster-1-job-0", patch = "[{\"op\":\"replace\",\"path\":\"/spec/bootstrap/className\",\"value\":\"com.nextbreakpoint.flink.jobs.stream.TestJob\"}]") != 0) {
            fail("Can't update job")
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Starting)).isTrue()
        }
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, jobName = "cluster-1-job-0", status = JobStatus.Started)).isTrue()
        }
    }
}