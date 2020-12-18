package com.nextbreakpoint.flink.integration

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.RestartPolicy
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.common.ScaleJobOptions
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobStatus
import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import io.kubernetes.client.openapi.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files
import kotlin.test.fail

@Tag("IntegrationTest")
class EnsureBehaviourTest : IntegrationSetup() {
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
        printJobManagerLogs()
        printTaskManagerLogs()
        printBootstrapJobLogs()
        describeResources()
    }

    @Test
    fun `should behave as expected`() {
        // A long test like this is not ideal, however it is faster to do it like this, because the tests are very slow to setup and teardown.

        `Should start the cluster and its jobs when the cluster is created`()

        val savepointPath01 = `Should create a savepoint and stop the job`()

        `Should restart the job from the savepoint`(savepointPath01)

        `Should stop the job but not create a savepoint`(savepointPath01)

        `Should restart the job but ignore the savepoint`()

        `Should trigger savepoint when requested`()

        `Should forget savepoint when requested`()

        `Should create savepoint periodically`()

        `Should start the cluster and its jobs when cluster has multiple jobs`()

        `Should return job details`()

        `Should return job metrics`()

        `Should return JobManager details`()

        `Should return TaskManagers`()

        `Should return TaskManager details`()

        `Should return TaskManager metrics`()

        `Should return cluster status`()

        `Should delete cluster and jobs`()

        val (savepointPath11, savepointPath12) = `Should create savepoints and stop all jobs when the cluster is stopped requesting savepoint`()

        `Should restart the jobs from the savepoints when the cluster is restarted requiring savepoint and job restart policy is always`()

        `Should stop all jobs but not create savepoints when the cluster is stopped without requesting savepoint`(savepointPath11, savepointPath12)

        `Should restart the jobs but ignore the savepoints when the cluster is restarted without requiring savepoint and job restart policy is always`()

        `Should recreate missing resources`()

        `Should create resources`()

        `Should delete resources`()

        `Should stop job when batch job has finished`()

        val savepointPath21 = `Should create a savepoint and stop a job`()

        `Should restart a job from the savepoint`(savepointPath21)

        `Should stop a job but not create a savepoint`(savepointPath21)

//        updateRestartPolicy(clusterName = "cluster-2", index = 0, restartPolicy = RestartPolicy.Always)
//        updateRestartPolicy(clusterName = "cluster-2", index = 1, restartPolicy = RestartPolicy.Always)

        `Should restart a job but ignore the savepoint`()

        `Should stop job when job fails because invalid bootstrap image`()

        `Should stop job when job fails because invalid bootstrap className`()

        `Should start the cluster without jobs`()

        `Should update clusters when servicemode changed`()

        `Should update clusters when taskslots changed`()

        `Should update clusters when taskmanagers changed`()

        `Should update clusters when scaling taskmanagers down`()

        `Should update clusters when scaling taskmanagers up`()

        `Should update clusters when scaling taskmanagers down to zero`()

        `Should update clusters when scaling job parallelism`()

        `Should delete cluster with multiple jobs`()

        //TODO verify that when parallelism is zero the job is stopped (work needed)

        //TODO verify that when total parallelism is zero the cluster is stopped (work needed)

        //TODO verify that job can be stopped when a savepoint is already in progress

        //TODO verify that scale limits are applied to job parallelism and taskmanagers

        //TODO verify that job restarts when status is failed and restart policy is OnlyIfFailed

        //TODO create job which is not part of a deployment and verify that everything works
    }

    private fun `Should delete cluster with multiple jobs`() {
        println("Should delete cluster with multiple jobs...")

        deleteResource(namespace = namespace, path = "integration/deployment-2.yaml")
        assertJobStopping(clusterName = "cluster-2", jobName = "job-1")
        assertJobStopping(clusterName = "cluster-2", jobName = "job-2")
        assertClusterStopping("cluster-2")
        assertJobDeleted(clusterName = "cluster-2", jobName = "job-1")
        assertJobDeleted(clusterName = "cluster-2", jobName = "job-2")
        assertClusterDeleted("cluster-2")
    }

    private fun `Should update clusters when scaling job parallelism`() {
        println("Should update clusters when scaling job parallelism...")

        awaitUntilAsserted(timeout = 180) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updated)).isTrue()
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-2", taskManagers = 2)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-2")
            assertThat(latestStatus.taskManagers).isEqualTo(2)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(2)
            assertThat(latestStatus.taskSlots).isEqualTo(2)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(4)
        }

        scaleJob(clusterName = "cluster-2", jobName = "job-1", options = ScaleJobOptions(parallelism = 4))
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updating)).isTrue()
        }

        awaitUntilAsserted(timeout = 360) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updated)).isTrue()
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-2", taskManagers = 3)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-2")
            assertThat(latestStatus.taskManagers).isEqualTo(3)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(3)
            assertThat(latestStatus.taskSlots).isEqualTo(2)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(6)
        }

        scaleJob(clusterName = "cluster-2", jobName = "job-2", options = ScaleJobOptions(parallelism = 4))
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updating)).isTrue()
        }

        awaitUntilAsserted(timeout = 360) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updated)).isTrue()
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-2", taskManagers = 4)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-2")
            assertThat(latestStatus.taskManagers).isEqualTo(4)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(4)
            assertThat(latestStatus.taskSlots).isEqualTo(2)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(8)
        }

        scaleJob(clusterName = "cluster-2", jobName = "job-1", options = ScaleJobOptions(parallelism = 1))
        scaleJob(clusterName = "cluster-2", jobName = "job-2", options = ScaleJobOptions(parallelism = 1))
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updating)).isTrue()
        }

        awaitUntilAsserted(timeout = 480) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-2", status = ResourceStatus.Updated)).isTrue()
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-2", taskManagers = 1)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-2")
            assertThat(latestStatus.taskManagers).isEqualTo(1)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(1)
            assertThat(latestStatus.taskSlots).isEqualTo(2)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(2)
        }
    }

    private fun `Should update clusters when scaling taskmanagers down to zero`() {
        println("Should update clusters when scaling taskmanagers down to zero...")

        scaleCluster(clusterName = "cluster-0", options = ScaleClusterOptions(taskManagers = 0))
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updated)).isTrue()
        }

        awaitUntilAsserted(timeout = 60) {
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 0)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-0")
            assertThat(latestStatus.taskManagers).isEqualTo(0)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(0)
            assertThat(latestStatus.taskSlots).isEqualTo(4)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(0)
        }
    }

    private fun `Should update clusters when scaling taskmanagers up`() {
        println("Should update clusters when scaling taskmanagers up...")

        scaleCluster(clusterName = "cluster-0", options = ScaleClusterOptions(taskManagers = 2))
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updated)).isTrue()
        }

        awaitUntilAsserted(timeout = 60) {
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 2)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-0")
            assertThat(latestStatus.taskManagers).isEqualTo(2)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(2)
            assertThat(latestStatus.taskSlots).isEqualTo(4)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(8)
        }
    }

    private fun `Should update clusters when scaling taskmanagers down`() {
        println("Should update clusters when scaling taskmanagers down...")

        scaleCluster(clusterName = "cluster-0", options = ScaleClusterOptions(taskManagers = 1))
        awaitUntilAsserted(timeout = 60, delay = 1, interval = 1) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updating)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = "cluster-0", status = ResourceStatus.Updated)).isTrue()
        }

        awaitUntilAsserted(timeout = 60) {
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 1)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-0")
            assertThat(latestStatus.taskManagers).isEqualTo(1)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(1)
            assertThat(latestStatus.taskSlots).isEqualTo(4)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(4)
        }
    }

    private fun `Should update clusters when taskmanagers changed`() {
        println("Should update clusters when taskmanagers changed...")

        updateTaskManagers(clusterName = "cluster-0", 2)

        assertClusterStarted("cluster-0")

        awaitUntilAsserted(timeout = 120) {
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 2)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-0")
            assertThat(latestStatus.taskManagers).isEqualTo(2)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(2)
            assertThat(latestStatus.taskSlots).isEqualTo(4)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(8)
        }
    }

    private fun `Should update clusters when taskslots changed`() {
        println("Should update clusters when taskslots changed...")

        updateTaskSlots(clusterName = "cluster-0", 4)

        assertClusterStopping("cluster-0")
        assertClusterStarted("cluster-0")

        awaitUntilAsserted(timeout = 60) {
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 1)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-0")
            assertThat(latestStatus.taskManagers).isEqualTo(1)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(1)
            assertThat(latestStatus.taskSlots).isEqualTo(4)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(4)
        }
    }

    private fun `Should update clusters when servicemode changed`() {
        println("Should update clusters when servicemode changed...")

        updateServiceMode(clusterName = "cluster-0", "ClusterIP")

        assertClusterStopping("cluster-0")
        assertClusterStarted("cluster-0")

        awaitUntilAsserted(timeout = 60) {
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 1)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-0")
            assertThat(latestStatus.taskManagers).isEqualTo(1)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(1)
            assertThat(latestStatus.serviceMode).isEqualTo("ClusterIP")
            assertThat(latestStatus.taskSlots).isEqualTo(2)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(2)
        }
    }

    private fun `Should stop job when job fails because invalid bootstrap className`() {
        println("Should stop job when job fails because invalid bootstrap className...")

        if (updateDeployment(namespace = namespace, name = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/bootstrap/className\",\"value\":\"wrongclassname\"}]") != 0) {
            fail("Can't update deployment")
        }

        assertJobStopping(clusterName = "cluster-2", jobName = "job-1")
        assertJobStarting(clusterName = "cluster-2", jobName = "job-1")
        assertJobStopped(clusterName = "cluster-2", jobName = "job-1")

        if (updateDeployment(namespace = namespace, name = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/bootstrap/className\",\"value\":\"com.nextbreakpoint.flink.jobs.stream.TestJob\"}]") != 0) {
            fail("Can't update deployment")
        }

        assertJobStarting(clusterName = "cluster-2", jobName = "job-1")
        assertJobStarted(clusterName = "cluster-2", jobName = "job-1")
    }

    private fun `Should stop job when job fails because invalid bootstrap image`() {
        println("Should stop job when job fails because invalid bootstrap image...")

        if (updateDeployment(namespace = namespace, name = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/bootstrap/image\",\"value\":\"wrong/jobs:latest\"}]") != 0) {
            fail("Can't update deployment")
        }
        assertJobStopping(clusterName = "cluster-2", jobName = "job-1")
        assertJobStarting(clusterName = "cluster-2", jobName = "job-1")
        assertJobStopped(clusterName = "cluster-2", jobName = "job-1")

        if (updateDeployment(namespace = namespace, name = "cluster-2", patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/0/spec/bootstrap/image\",\"value\":\"integration/jobs:latest\"}]") != 0) {
            fail("Can't update deployment")
        }

        assertJobStarting(clusterName = "cluster-2", jobName = "job-1")
        assertJobStarted(clusterName = "cluster-2", jobName = "job-1")
    }

    private fun `Should restart a job but ignore the savepoint`() {
        println("Should restart a job but ignore the savepoint...")

        startJob(clusterName = "cluster-2", jobName = "job-1", options = StartOptions(withoutSavepoint = true))
        assertJobStarted(clusterName = "cluster-2", jobName = "job-1")
        val savepointPath24 = getSavepointPath(clusterName = "cluster-2", jobName = "job-1")
        assertThat(savepointPath24).isBlank()
    }

    private fun `Should stop a job but not create a savepoint`(savepointPath21: String?) {
        println("Should stop a job but not create a savepoint...")

        stopJob(clusterName = "cluster-2", jobName = "job-1", options = StopOptions(withoutSavepoint = true))
        assertJobStopped(clusterName = "cluster-2", jobName = "job-1")
        val savepointPath23 = getSavepointPath(clusterName = "cluster-2", jobName = "job-1")
        assertThat(savepointPath23).isEqualTo(savepointPath21)
    }

    private fun `Should restart a job from the savepoint`(savepointPath21: String?) {
        println("Should restart a job from the savepoint...")

        startJob(clusterName = "cluster-2", jobName = "job-1", options = StartOptions(withoutSavepoint = false))
        assertJobStarted(clusterName = "cluster-2", jobName = "job-1")
        val savepointPath22 = getSavepointPath(clusterName = "cluster-2", jobName = "job-1")
        assertThat(savepointPath22).isEqualTo(savepointPath21)
    }

    private fun `Should create a savepoint and stop a job`(): String? {
        println("Should create a savepoint and stop a job...")

        val savepointPath20 = getSavepointPath(clusterName = "cluster-2", jobName = "job-1")
        stopJob(clusterName = "cluster-2", jobName = "job-1", options = StopOptions(withoutSavepoint = false))
        assertJobStopped(clusterName = "cluster-2", jobName = "job-1")
        val savepointPath21 = getSavepointPath(clusterName = "cluster-2", jobName = "job-1")
        assertThat(savepointPath21).isNotEqualTo(savepointPath20)
        return savepointPath21
    }

    private fun `Should stop job when batch job has finished`() {
        println("Should stop job when batch job has finished...")

        createResource(namespace = namespace, path = "integration/deployment-3.yaml")
        assertClusterStarted("cluster-3")
        assertJobStarted(clusterName = "cluster-3", jobName = "job-0")
        assertJobStopped(clusterName = "cluster-3", jobName = "job-0")
        assertClusterStarted("cluster-3")
        awaitUntilAsserted(timeout = 120) {
            assertThat(hasClusterJobStatus(namespace = namespace, name = "cluster-3-job-0", status = "FINISHED")).isTrue()
        }
        awaitUntilAsserted(timeout = 240) {
            assertThat(getTaskManagers(namespace = namespace, name = "cluster-3")).isEqualTo(0)
        }
//        deleteCluster(clusterName = "cluster-3")
//        assertClusterDeleted(clusterName = "cluster-3")
    }

    private fun `Should delete resources`() {
        println("Should delete resources...")

        val jobSpecJson = String(Files.readAllBytes(File("integration/job-spec.json").toPath()))
        val jobSpec = JSON().deserialize<V1FlinkJobSpec>(jobSpecJson, jobSpecTypeToken.type)
        deleteJob(clusterName = "cluster-0", jobName = "job-0")
        assertJobDeleted(clusterName = "cluster-0", jobName = "job-0")
        createJob(clusterName = "cluster-0", jobName = "job-0", spec = jobSpec)
        assertJobStarted(clusterName = "cluster-0", jobName = "job-0")
        deleteCluster(clusterName = "cluster-0")
        assertClusterTerminated(clusterName = "cluster-0")
        deleteJob(clusterName = "cluster-0", jobName = "job-0")
        assertClusterDeleted(clusterName = "cluster-0")
        assertJobDeleted(clusterName = "cluster-0", jobName = "job-0")
    }

    private fun `Should create resources`() {
        println("Should create resources...")

        val clusterSpecJson = String(Files.readAllBytes(File("integration/cluster-spec.json").toPath()))
        val clusterSpec = JSON().deserialize<V1FlinkClusterSpec>(clusterSpecJson, clusterSpecTypeToken.type)
        createCluster(clusterName = "cluster-0", spec = clusterSpec)
        assertClusterStarted("cluster-0")

        val jobSpecJson = String(Files.readAllBytes(File("integration/job-spec.json").toPath()))
        val jobSpec = JSON().deserialize<V1FlinkJobSpec>(jobSpecJson, jobSpecTypeToken.type)
        createJob(clusterName = "cluster-0", jobName = "job-0", spec = jobSpec)
        assertJobStarted(clusterName = "cluster-0", jobName = "job-0")
    }

    private fun `Should recreate missing resources`() {
        println("Should recreate missing resources...")

        assertClusterExists(clusterName = "cluster-2")
        assertJobExists(clusterName = "cluster-2", jobName = "job-1")

        assertJobStarted(clusterName = "cluster-2", jobName = "job-1")
        deleteJob(clusterName = "cluster-2", jobName = "job-1")
        assertJobStarting(clusterName = "cluster-2", jobName = "job-1")
        assertJobStarted(clusterName = "cluster-2", jobName = "job-1")

        assertClusterStarted("cluster-2")
        deleteCluster(clusterName = "cluster-2")
        assertClusterStarting("cluster-2")
        assertClusterStarted("cluster-2")
    }

    private fun `Should restart the jobs but ignore the savepoints when the cluster is restarted without requiring savepoint and job restart policy is always`() {
        println("Should restart the jobs but ignore the savepoints when the cluster is restarted without requiring savepoint and job restart policy is always...")

        updateRestartPolicy(clusterName = "cluster-2", index = 0, restartPolicy = RestartPolicy.Always)
        startCluster(clusterName = "cluster-2", options = StartOptions(withoutSavepoint = true))
        assertClusterStarted("cluster-2")
        assertJobStarted(clusterName = "cluster-2", jobName = "job-1")
        assertJobStarted(clusterName = "cluster-2", jobName = "job-2")

        awaitUntilAsserted(timeout = 120) {
            assertThat(getTaskManagers(namespace = namespace, name = "cluster-2")).isEqualTo(2)
        }

        val savepointPath15 = getSavepointPath(clusterName = "cluster-2", jobName = "job-1")
        val savepointPath16 = getSavepointPath(clusterName = "cluster-2", jobName = "job-2")
        assertThat(savepointPath15).isBlank()
        assertThat(savepointPath16).isBlank()
    }

    private fun `Should stop all jobs but not create savepoints when the cluster is stopped without requesting savepoint`(savepointPath11: String?, savepointPath12: String?) {
        println("Should stop all jobs but not create savepoints when the cluster is stopped without requesting savepoint...")

        stopCluster(clusterName = "cluster-2", options = StopOptions(withoutSavepoint = true))
        assertJobStopped(clusterName = "cluster-2", jobName = "job-1")
        assertJobStopped(clusterName = "cluster-2", jobName = "job-2")
        assertClusterStopped("cluster-2")

        awaitUntilAsserted(timeout = 120) {
            assertThat(getTaskManagers(namespace = namespace, name = "cluster-2")).isEqualTo(0)
        }

        val savepointPath13 = getSavepointPath(clusterName = "cluster-2", jobName = "job-1")
        val savepointPath14 = getSavepointPath(clusterName = "cluster-2", jobName = "job-2")
        assertThat(savepointPath13).isEqualTo(savepointPath11)
        assertThat(savepointPath14).isEqualTo(savepointPath12)
    }

    private fun `Should restart the jobs from the savepoints when the cluster is restarted requiring savepoint and job restart policy is always`() {
        println("Should restart the jobs from the savepoints when the cluster is restarted requiring savepoint and job restart policy is always...")

        updateRestartPolicy(clusterName = "cluster-2", index = 1, restartPolicy = RestartPolicy.Always)
        startCluster(clusterName = "cluster-2", options = StartOptions(withoutSavepoint = false))
        assertClusterStarted("cluster-2")
        assertJobStopped(clusterName = "cluster-2", jobName = "job-1")
        assertJobStarted(clusterName = "cluster-2", jobName = "job-2")

        awaitUntilAsserted(timeout = 120) {
            assertThat(getTaskManagers(namespace = namespace, name = "cluster-2")).isEqualTo(1)
        }
    }

    private fun `Should create savepoints and stop all jobs when the cluster is stopped requesting savepoint`(): Pair<String?, String?> {
        println("Should create savepoints and stop all jobs when the cluster is stopped requesting savepoint...")

        stopCluster(clusterName = "cluster-2", options = StopOptions(withoutSavepoint = false))
        assertJobStopped(clusterName = "cluster-2", jobName = "job-1")
        assertJobStopped(clusterName = "cluster-2", jobName = "job-2")
        assertClusterStopped("cluster-2")

        awaitUntilAsserted(timeout = 120) {
            assertThat(getTaskManagers(namespace = namespace, name = "cluster-2")).isEqualTo(0)
        }

        val savepointPath11 = getSavepointPath(clusterName = "cluster-2", jobName = "job-1")
        val savepointPath12 = getSavepointPath(clusterName = "cluster-2", jobName = "job-2")
        assertThat(savepointPath11).isNotEmpty()
        assertThat(savepointPath12).isNotEmpty()
        return Pair(savepointPath11, savepointPath12)
    }

    private fun `Should delete cluster and jobs`() {
        println("Should delete cluster and jobs...")

        deleteResource(namespace = namespace, path = "integration/deployment-1.yaml")
        assertJobStopping(clusterName = "cluster-1", jobName = "job-0")
        assertClusterStopping("cluster-1")
        assertJobDeleted(clusterName = "cluster-1", jobName = "job-0")
        assertClusterDeleted("cluster-1")
    }

    private fun `Should return cluster status`() {
        println("Should return cluster status...")

        val status1 = getLatestClusterStatus("cluster-1")
        assertThat(status1.supervisorStatus).isEqualTo(ClusterStatus.Started.toString())
        assertThat(status1.taskManagers).isEqualTo(1)
        assertThat(status1.taskManagerReplicas).isEqualTo(1)
        assertThat(status1.serviceMode).isEqualTo("NodePort")
        assertThat(status1.taskSlots).isEqualTo(1)
        assertThat(status1.totalTaskSlots).isEqualTo(1)

        val status2 = getLatestClusterStatus("cluster-2")
        assertThat(status2.supervisorStatus).isEqualTo(ClusterStatus.Started.toString())
        assertThat(status2.taskManagers).isEqualTo(2)
        assertThat(status2.taskManagerReplicas).isEqualTo(2)
        assertThat(status2.serviceMode).isEqualTo("ClusterIP")
        assertThat(status2.taskSlots).isEqualTo(2)
        assertThat(status2.totalTaskSlots).isEqualTo(4)
    }

    private fun `Should return TaskManager metrics`() {
        println("Should return TaskManager metrics...")

        awaitUntilAsserted(timeout = 10) {
            val listResponse = getTaskManagers(clusterName = "cluster-2")
            assertThat(listResponse["status"] as String?).isEqualTo("OK")
            val taskmanagers = JSON().deserialize<List<TaskManagerInfo>>(listResponse["output"] as String, taskmanagersTypeToken.type)
            val metricsResponse1 = getTaskManagerMetrics(clusterName = "cluster-2", taskmanagerId = TaskManagerId(taskmanagers[0].id))
            println(metricsResponse1)
            assertThat(metricsResponse1["status"] as String?).isEqualTo("OK")
            val metrics1 = metricsResponse1["output"] as String
            assertThat(metrics1).isNotBlank()
            val metricsResponse2 = getTaskManagerMetrics(clusterName = "cluster-2", taskmanagerId = TaskManagerId(taskmanagers[1].id))
            println(metricsResponse2)
            assertThat(metricsResponse2["status"] as String?).isEqualTo("OK")
            val metrics2 = metricsResponse2["output"] as String
            assertThat(metrics2).isNotBlank()
        }
    }

    private fun `Should return TaskManager details`() {
        println("Should return TaskManager details...")

        awaitUntilAsserted(timeout = 10) {
            val listResponse = getTaskManagers(clusterName = "cluster-1")
            assertThat(listResponse["status"] as String?).isEqualTo("OK")
            val taskmanagers = JSON().deserialize<List<TaskManagerInfo>>(listResponse["output"] as String, taskmanagersTypeToken.type)
            val detailsResponse = getTaskManagerDetails(clusterName = "cluster-1", taskmanagerId = TaskManagerId(taskmanagers[0].id))
            println(detailsResponse)
            assertThat(detailsResponse["status"] as String?).isEqualTo("OK")
            val details = detailsResponse["output"] as String
            assertThat(details).isNotBlank()
        }
    }

    private fun `Should return TaskManagers`() {
        println("Should return TaskManagers...")

        awaitUntilAsserted(timeout = 10) {
            val response1 = getTaskManagers(clusterName = "cluster-1")
            assertThat(response1["status"] as String?).isEqualTo("OK")
            val taskmanagers1 = JSON().deserialize<List<TaskManagerInfo>>(response1["output"] as String, taskmanagersTypeToken.type)
            assertThat(taskmanagers1).hasSize(1)
            val response2 = getTaskManagers(clusterName = "cluster-2")
            assertThat(response2["status"] as String?).isEqualTo("OK")
            val taskmanagers2 = JSON().deserialize<List<TaskManagerInfo>>(response2["output"] as String, taskmanagersTypeToken.type)
            assertThat(taskmanagers2).hasSize(2)
        }
    }

    private fun `Should return JobManager details`() {
        println("Should return JobManager details...")

        awaitUntilAsserted(timeout = 10) {
            val response = getJobManagerMetrics(clusterName = "cluster-1")
            assertThat(response["status"] as String?).isEqualTo("OK")
            val metrics = response["output"] as String
            assertThat(metrics).isNotBlank()
        }
    }

    private fun `Should return job metrics`() {
        println("Should return job metrics...")

        awaitUntilAsserted(timeout = 10) {
            val response = getJobMetrics(clusterName = "cluster-1", jobName = "job-0")
            assertThat(response["status"] as String?).isEqualTo("OK")
            val metrics = response["output"] as String
            assertThat(metrics).isNotBlank()
        }
    }

    private fun `Should return job details`() {
        println("Should return job details...")

        awaitUntilAsserted(timeout = 10) {
            val response = getJobDetails(clusterName = "cluster-1", jobName = "job-0")
            assertThat(response["status"] as String?).isEqualTo("OK")
            val details = response["output"] as String
            assertThat(details).isNotBlank()
        }
    }

    private fun `Should start the cluster and its jobs when cluster has multiple jobs`() {
        println("Should start the cluster and its jobs when cluster has multiple jobs...")

        createResource(namespace = namespace, path = "integration/deployment-2.yaml")
        assertClusterStarted(clusterName = "cluster-2")
        assertJobStarted(clusterName = "cluster-2", jobName = "job-1")
        assertClusterStarted(clusterName = "cluster-2")
        assertJobStarted(clusterName = "cluster-2", jobName = "job-2")

        awaitUntilAsserted(timeout = 60) {
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-2", taskManagers = 2)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-2")
            assertThat(latestStatus.taskManagers).isEqualTo(2)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(2)
            assertThat(latestStatus.serviceMode).isEqualTo("ClusterIP")
            assertThat(latestStatus.taskSlots).isEqualTo(2)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(4)
        }
    }

    private fun `Should create savepoint periodically`() {
        updateSavepointInterval(clusterName = "cluster-1", index = 0, savepointInterval = 20)

        awaitUntilAsserted(timeout = 120) {
            val latestStatus = getLatestJobStatus("cluster-1", "job-0")
            assertThat(latestStatus.savepointPath).isNotBlank()
        }

        val previousJobStatus = getLatestJobStatus("cluster-1", "job-0")

        awaitUntilAsserted(timeout = 120) {
            val latestStatus = getLatestJobStatus("cluster-1", "job-0")
            assertThat(latestStatus.savepointPath).isNotEqualTo(previousJobStatus.savepointPath)
            assertThat(latestStatus.savepointTimestamp).isGreaterThan(previousJobStatus.savepointTimestamp)
        }

        updateSavepointInterval(clusterName = "cluster-1", index = 0, savepointInterval = 0)
    }

    private fun `Should forget savepoint when requested`() {
        println("Should forget savepoint when requested...")

        forgetSavepoint(clusterName = "cluster-1", jobName = "job-0")

        awaitUntilAsserted(timeout = 120) {
            val latestStatus = getLatestJobStatus("cluster-1", "job-0")
            assertThat(latestStatus.savepointPath).isBlank()
        }
    }

    private fun `Should trigger savepoint when requested`() {
        println("Should trigger savepoint when requested...")

        awaitUntilAsserted(timeout = 60) {
            val latestStatus = getLatestJobStatus("cluster-1", "job-0")
            assertThat(latestStatus.savepointPath).isBlank()
        }

        triggerSavepoint(clusterName = "cluster-1", jobName = "job-0")

        awaitUntilAsserted(timeout = 120) {
            val latestStatus = getLatestJobStatus("cluster-1", "job-0")
            assertThat(latestStatus.savepointPath).isNotBlank()
        }
    }

    private fun `Should restart the job but ignore the savepoint`() {
        println("Should restart the job but ignore the savepoint...")

        startJob(clusterName = "cluster-1", jobName = "job-0", options = StartOptions(withoutSavepoint = true))
        assertJobStarted(clusterName = "cluster-1", jobName = "job-0")
        val savepointPath4 = getSavepointPath(clusterName = "cluster-1", jobName = "job-0")
        assertThat(savepointPath4).isBlank()
    }

    private fun `Should stop the job but not create a savepoint`(savepointPath1: String?) {
        println("Should stop the job but not create a savepoint...")

        stopJob(clusterName = "cluster-1", jobName = "job-0", options = StopOptions(withoutSavepoint = true))
        assertJobStopped(clusterName = "cluster-1", jobName = "job-0")
        val savepointPath3 = getSavepointPath(clusterName = "cluster-1", jobName = "job-0")
        assertThat(savepointPath3).isEqualTo(savepointPath1)
    }

    private fun `Should restart the job from the savepoint`(savepointPath1: String?) {
        println("Should restart the job from the savepoint...")

        startJob(clusterName = "cluster-1", jobName = "job-0", options = StartOptions(withoutSavepoint = false))
        assertJobStarted(clusterName = "cluster-1", jobName = "job-0")
        val savepointPath2 = getSavepointPath(clusterName = "cluster-1", jobName = "job-0")
        assertThat(savepointPath2).isEqualTo(savepointPath1)
    }

    private fun `Should create a savepoint and stop the job`(): String? {
        println("Should create a savepoint and stop the job...")

        updateRestartPolicy(clusterName = "cluster-1", index = 0, restartPolicy = RestartPolicy.Always)
        val savepointPath0 = getSavepointPath(clusterName = "cluster-1", jobName = "job-0")
        assertThat(savepointPath0).isBlank()
        stopJob(clusterName = "cluster-1", jobName = "job-0", options = StopOptions(withoutSavepoint = false))
        assertJobStopped(clusterName = "cluster-1", jobName = "job-0")
        val savepointPath1 = getSavepointPath(clusterName = "cluster-1", jobName = "job-0")
        assertThat(savepointPath1).isNotBlank()
        return savepointPath1
    }

    private fun `Should start the cluster and its jobs when the cluster is created`() {
        println("Should start the cluster and its jobs when the cluster is created...")

        createResource(namespace = namespace, path = "integration/deployment-1.yaml")
        assertClusterStarting(clusterName = "cluster-1")
        assertJobStarting(clusterName = "cluster-1", jobName = "job-0")
        assertClusterStarted(clusterName = "cluster-1")
        assertJobStarted(clusterName = "cluster-1", jobName = "job-0")

        awaitUntilAsserted(timeout = 60) {
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-1", taskManagers = 1)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-1")
            assertThat(latestStatus.taskManagers).isEqualTo(1)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(1)
            assertThat(latestStatus.serviceMode).isEqualTo("NodePort")
            assertThat(latestStatus.taskSlots).isEqualTo(1)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(1)
        }
    }

    private fun `Should start the cluster without jobs`() {
        println("Should start the cluster without jobs...")

        createResource(namespace = namespace, path = "integration/deployment-0.yaml")
        assertClusterStarting(clusterName = "cluster-0")
        assertClusterStarted(clusterName = "cluster-0")

        awaitUntilAsserted(timeout = 60) {
            assertThat(hasTaskManagers(namespace = namespace, name = "cluster-0", taskManagers = 1)).isTrue()
            val latestStatus = getLatestClusterStatus(clusterName = "cluster-0")
            assertThat(latestStatus.taskManagers).isEqualTo(1)
            assertThat(latestStatus.taskManagerReplicas).isEqualTo(1)
            assertThat(latestStatus.serviceMode).isEqualTo("NodePort")
            assertThat(latestStatus.taskSlots).isEqualTo(2)
            assertThat(latestStatus.totalTaskSlots).isEqualTo(2)
        }
    }

    private fun assertClusterExists(clusterName: String) {
        awaitUntilAsserted(timeout = 60) {
            assertThat(clusterExists(namespace = namespace, name = clusterName)).isTrue()
        }
    }

    private fun assertClusterStarted(clusterName: String) {
        awaitUntilAsserted(timeout = 60) {
            assertThat(clusterExists(namespace = namespace, name = clusterName)).isTrue()
        }
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, name = clusterName, status = ClusterStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fc", name = clusterName, status = ResourceStatus.Updated)).isTrue()
        }
    }

    private fun assertClusterStopped(clusterName: String) {
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, name = clusterName, status = ClusterStatus.Stopped)).isTrue()
        }
    }

    private fun assertClusterTerminated(clusterName: String) {
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, name = clusterName, status = ClusterStatus.Terminated)).isTrue()
        }
    }

    private fun assertClusterStarting(clusterName: String) {
        awaitUntilAsserted(timeout = 360) {
            assertThat(hasClusterStatus(namespace = namespace, name = clusterName, status = ClusterStatus.Starting)).isTrue()
        }
    }

    private fun assertClusterStopping(clusterName: String) {
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasClusterStatus(namespace = namespace, name = clusterName, status = ClusterStatus.Stopping)).isTrue()
        }
    }

    private fun assertClusterDeleted(clusterName: String) {
        awaitUntilAsserted(timeout = 360) {
            assertThat(clusterExists(namespace = namespace, name = clusterName)).isFalse()
        }
    }

    private fun assertJobExists(clusterName: String, jobName: String) {
        awaitUntilAsserted(timeout = 60) {
            assertThat(jobExists(namespace = namespace, name = "$clusterName-$jobName")).isTrue()
        }
    }

    private fun assertJobStarted(clusterName: String, jobName: String) {
        awaitUntilAsserted(timeout = 60) {
            assertThat(jobExists(namespace = namespace, name = "$clusterName-$jobName")).isTrue()
        }
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, name = "$clusterName-$jobName", status = JobStatus.Started)).isTrue()
            assertThat(hasResourceStatus(namespace = namespace, resource = "fj", name = "$clusterName-$jobName", status = ResourceStatus.Updated)).isTrue()
        }
    }

    private fun assertJobStopped(clusterName: String, jobName: String) {
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, name = "$clusterName-$jobName", status = JobStatus.Stopped)).isTrue()
        }
    }

    private fun assertJobTerminated(clusterName: String, jobName: String) {
        awaitUntilAsserted(timeout = 180) {
            assertThat(hasJobStatus(namespace = namespace, name = "$clusterName-$jobName", status = JobStatus.Terminated)).isTrue()
        }
    }

    private fun assertJobStarting(clusterName: String, jobName: String) {
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasJobStatus(namespace = namespace, name = "$clusterName-$jobName", status = JobStatus.Starting)).isTrue()
        }
    }

    private fun assertJobStopping(clusterName: String, jobName: String) {
        awaitUntilAsserted(timeout = 60) {
            assertThat(hasJobStatus(namespace = namespace, name = "$clusterName-$jobName", status = JobStatus.Stopping)).isTrue()
        }
    }

    private fun assertJobDeleted(clusterName: String, jobName: String) {
        awaitUntilAsserted(timeout = 180) {
            assertThat(jobExists(namespace = namespace, name = "$clusterName-$jobName")).isFalse()
        }
    }

    private fun updateServiceMode(clusterName: String, serviceMode: String) {
        if (updateDeployment(namespace = namespace, name = clusterName, patch = "[{\"op\":\"replace\",\"path\":\"/spec/cluster/jobManager/serviceMode\",\"value\":\"$serviceMode\"}]") != 0) {
            fail("Can't update deployment")
        }
    }

    private fun updateTaskSlots(clusterName: String, taskSlots: Int) {
        if (updateDeployment(namespace = namespace, name = clusterName, patch = "[{\"op\":\"replace\",\"path\":\"/spec/cluster/taskManager/taskSlots\",\"value\":$taskSlots}]") != 0) {
            fail("Can't update deployment")
        }
    }

    private fun updateTaskManagers(clusterName: String, taskManagers: Int) {
        if (updateCluster(namespace = namespace, name = clusterName, patch = "[{\"op\":\"replace\",\"path\":\"/spec/taskManagers\",\"value\":$taskManagers}]") != 0) {
            fail("Can't update cluster")
        }
    }

    private fun updateRestartPolicy(clusterName: String, index: Int, restartPolicy: RestartPolicy) {
        if (updateDeployment(namespace = namespace, name = clusterName, patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/$index/spec/restart/restartPolicy\",\"value\":\"${restartPolicy.name}\"}]") != 0) {
            fail("Can't update deployment")
        }
    }

    private fun updateSavepointInterval(clusterName: String, index: Int, savepointInterval: Long) {
        if (updateDeployment(namespace = namespace, name = clusterName, patch = "[{\"op\":\"replace\",\"path\":\"/spec/jobs/$index/spec/savepoint/savepointInterval\",\"value\":$savepointInterval}]") != 0) {
            fail("Can't update deployment")
        }
    }

    private fun getSavepointPath(clusterName: String, jobName: String) = getSavepointPath(namespace = namespace, name = "$clusterName-$jobName")

    private fun getLatestClusterStatus(clusterName: String): V1FlinkClusterStatus {
        val response = getClusterStatus(clusterName = clusterName)
        assertThat(response["status"] as String?).isEqualTo("OK")
        return JSON().deserialize(response["output"] as String, clusterStatusTypeToken.type)
    }

    private fun getLatestJobStatus(clusterName: String, jobName: String): V1FlinkJobStatus {
        val response = getJobStatus(clusterName = clusterName, jobName = jobName)
        assertThat(response["status"] as String?).isEqualTo("OK")
        return JSON().deserialize(response["output"] as String, jobStatusTypeToken.type)
    }
}
