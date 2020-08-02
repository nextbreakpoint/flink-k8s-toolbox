package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.joda.time.DateTime
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.spy
import org.mockito.Mockito.times
import org.mockito.Mockito.verify

class SupervisorTest {
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val clusterSelector = ClusterSelector(name = "test", namespace = "flink", uuid = "123")
    private val resources = SupervisorCachedResources(
        flinkCluster = cluster,
        bootstrapJob = TestFactory.aBootstrapJob(cluster),
        service = TestFactory.aJobManagerService(cluster),
        jobmanagerPods = setOf(TestFactory.aJobManagerPod(cluster,"1")),
        taskmanagerPods = setOf(TestFactory.aTaskManagerPod(cluster,"1"))
    )
    private val task = spy(DummyTask(cluster))
    private val tasks = mapOf(
        ClusterStatus.Unknown to task,
        ClusterStatus.Starting to task,
        ClusterStatus.Stopping to task,
        ClusterStatus.Updating to task,
        ClusterStatus.Scaling to task,
        ClusterStatus.Running to task,
        ClusterStatus.Failed to task,
        ClusterStatus.Suspended to task,
        ClusterStatus.Terminated to task,
        ClusterStatus.Cancelling to task
    )
    private val operationController = mock(OperationController::class.java)
    private val taskController = Supervisor.create(controller = operationController, tasks = tasks, loggerName = "Supervisor (" + clusterSelector.name + ")")

    @Test
    fun `should update savepoint request`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        taskController.reconcile(clusterSelector, resources)
        verify(task, times(1)).execute(any())
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    class DummyTask(val cluster: V1FlinkCluster) : Task() {
        override fun execute(context: TaskContext) {
            Status.setClusterStatus(cluster, ClusterStatus.Running)
        }
    }
}
