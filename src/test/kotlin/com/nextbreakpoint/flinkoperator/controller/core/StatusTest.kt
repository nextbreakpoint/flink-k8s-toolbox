package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class StatusTest {
    private val flinkCluster = TestFactory.aCluster(name = "test", namespace = "flink")

    @Test
    fun `initially a cluster doesn't have a current task`() {
        assertThat(Status.hasCurrentTask(flinkCluster)).isFalse()
        assertThat(Status.getCurrentTask(flinkCluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `cluster should have a current task after appending a new task`() {
        Status.appendTasks(flinkCluster, listOf(ClusterTask.InitialiseCluster))
        assertThat(Status.hasCurrentTask(flinkCluster)).isTrue()
    }

    @Test
    fun `should return default task`() {
        assertThat(Status.getCurrentTask(flinkCluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `should remain on default task`() {
        Status.selectNextTask(flinkCluster)
        assertThat(Status.getCurrentTask(flinkCluster)).isEqualTo(ClusterTask.ClusterHalted)
    }

    @Test
    fun `should return current task`() {
        Status.appendTasks(flinkCluster, listOf(ClusterTask.InitialiseCluster))
        Status.appendTasks(flinkCluster, listOf(ClusterTask.StartingCluster))
        assertThat(Status.getCurrentTask(flinkCluster)).isEqualTo(ClusterTask.InitialiseCluster)
    }

    @Test
    fun `should return next task`() {
        Status.appendTasks(flinkCluster, listOf(ClusterTask.InitialiseCluster))
        Status.appendTasks(flinkCluster, listOf(ClusterTask.StartingCluster))
        assertThat(Status.getNextOperatorTask(flinkCluster)).isEqualTo(ClusterTask.StartingCluster)
    }

    @Test
    fun `should advance to next task`() {
        Status.appendTasks(flinkCluster, listOf(ClusterTask.InitialiseCluster))
        Status.appendTasks(flinkCluster, listOf(ClusterTask.StartingCluster))
        Status.selectNextTask(flinkCluster)
        assertThat(Status.getCurrentTask(flinkCluster)).isEqualTo(ClusterTask.StartingCluster)
    }

    @Test
    fun `should remain on last task`() {
        Status.appendTasks(flinkCluster, listOf(ClusterTask.InitialiseCluster))
        Status.appendTasks(flinkCluster, listOf(ClusterTask.StartingCluster))
        Status.selectNextTask(flinkCluster)
        Status.selectNextTask(flinkCluster)
        assertThat(Status.getCurrentTask(flinkCluster)).isEqualTo(ClusterTask.StartingCluster)
    }

    @Test
    fun `should reset tasks`() {
        Status.appendTasks(flinkCluster, listOf(ClusterTask.InitialiseCluster))
        Status.resetTasks(flinkCluster, listOf(ClusterTask.StartingCluster))
        assertThat(Status.getCurrentTask(flinkCluster)).isEqualTo(ClusterTask.StartingCluster)
    }

    @Test
    fun `should update timestamp when appending tasks`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.appendTasks(flinkCluster, listOf(ClusterTask.InitialiseCluster))
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update timestamp when advancing tasks`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.selectNextTask(flinkCluster)
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update timestamp when resetting tasks`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.resetTasks(flinkCluster, listOf(ClusterTask.InitialiseCluster))
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store savepoint path and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        assertThat(Status.getSavepointTimestamp(flinkCluster)).isEqualTo(0)
        Status.setSavepointPath(flinkCluster, "/tmp/xxx")
        assertThat(Status.getSavepointPath(flinkCluster)).isEqualTo("/tmp/xxx")
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(Status.getSavepointTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store savepoint request and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        assertThat(Status.getSavepointTimestamp(flinkCluster)).isEqualTo(0)
        assertThat(Status.getSavepointRequest(flinkCluster)).isNull()
        Status.setSavepointRequest(flinkCluster, SavepointRequest("000", "XXX"))
        assertThat(Status.getSavepointRequest(flinkCluster)).isEqualTo(SavepointRequest("000", "XXX"))
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(Status.getSavepointTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update savepoint timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        assertThat(Status.getSavepointTimestamp(flinkCluster)).isEqualTo(0)
        Status.updateSavepointTimestamp(flinkCluster)
        assertThat(Status.getSavepointRequest(flinkCluster)).isNull()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(Status.getSavepointTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store operator status and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setTaskStatus(flinkCluster, TaskStatus.Awaiting)
        assertThat(Status.getCurrentTaskStatus(flinkCluster)).isEqualTo(TaskStatus.Awaiting)
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store cluster status and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setClusterStatus(flinkCluster, ClusterStatus.Checkpointing)
        assertThat(Status.getClusterStatus(flinkCluster)).isEqualTo(ClusterStatus.Checkpointing)
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store job manager digest and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setJobManagerDigest(flinkCluster, "XXX")
        assertThat(Status.getJobManagerDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task manager digest and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setTaskManagerDigest(flinkCluster, "XXX")
        assertThat(Status.getTaskManagerDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store flink image digest and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setRuntimeDigest(flinkCluster, "XXX")
        assertThat(Status.getRuntimeDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store flink job digest and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setBootstrapDigest(flinkCluster, "XXX")
        assertThat(Status.getBootstrapDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task attempts and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setTaskAttempts(flinkCluster, 2)
        assertThat(Status.getTaskAttempts(flinkCluster)).isEqualTo(2)
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task managers and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setTaskManagers(flinkCluster, 2)
        assertThat(Status.getTaskManagers(flinkCluster)).isEqualTo(2)
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store active task managers and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setActiveTaskManagers(flinkCluster, 2)
        assertThat(Status.getActiveTaskManagers(flinkCluster)).isEqualTo(2)
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task slots and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setTaskSlots(flinkCluster, 2)
        assertThat(Status.getTaskSlots(flinkCluster)).isEqualTo(2)
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store total task slots and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setTotalTaskSlots(flinkCluster, 4)
        assertThat(Status.getTotalTaskSlots(flinkCluster)).isEqualTo(4)
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store job parallelism and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setJobParallelism(flinkCluster, 4)
        assertThat(Status.getJobParallelism(flinkCluster)).isEqualTo(4)
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store label selector and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        Status.setLabelSelector(flinkCluster, "xxxx")
        assertThat(Status.getLabelSelector(flinkCluster)).isEqualTo("xxxx")
        assertThat(Status.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }
}
