package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class OperatorAnnotationsTest {
    private val flinkCluster = TestFactory.aCluster("test", "flink")

    @Test
    fun `initially a cluster doesn't have a current task`() {
        assertThat(OperatorAnnotations.hasCurrentTask(flinkCluster)).isFalse()
        assertThat(OperatorAnnotations.getCurrentTask(flinkCluster)).isEqualTo(OperatorTask.CLUSTER_HALTED)
    }

    @Test
    fun `cluster should have a current task after appending a new task`() {
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        assertThat(OperatorAnnotations.hasCurrentTask(flinkCluster)).isTrue()
    }

    @Test
    fun `should return default task`() {
        assertThat(OperatorAnnotations.getCurrentTask(flinkCluster)).isEqualTo(OperatorTask.CLUSTER_HALTED)
    }

    @Test
    fun `should remain on default task`() {
        OperatorAnnotations.selectNextTask(flinkCluster)
        assertThat(OperatorAnnotations.getCurrentTask(flinkCluster)).isEqualTo(OperatorTask.CLUSTER_HALTED)
    }

    @Test
    fun `should return current task`() {
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.STARTING_CLUSTER))
        assertThat(OperatorAnnotations.getCurrentTask(flinkCluster)).isEqualTo(OperatorTask.INITIALISE_CLUSTER)
    }

    @Test
    fun `should return next task`() {
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.STARTING_CLUSTER))
        assertThat(OperatorAnnotations.getNextOperatorTask(flinkCluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
    }

    @Test
    fun `should advance to next task`() {
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.STARTING_CLUSTER))
        OperatorAnnotations.selectNextTask(flinkCluster)
        assertThat(OperatorAnnotations.getCurrentTask(flinkCluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
    }

    @Test
    fun `should remain on last task`() {
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.STARTING_CLUSTER))
        OperatorAnnotations.selectNextTask(flinkCluster)
        OperatorAnnotations.selectNextTask(flinkCluster)
        assertThat(OperatorAnnotations.getCurrentTask(flinkCluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
    }

    @Test
    fun `should reset tasks`() {
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        OperatorAnnotations.resetTasks(flinkCluster, listOf(OperatorTask.STARTING_CLUSTER))
        assertThat(OperatorAnnotations.getCurrentTask(flinkCluster)).isEqualTo(OperatorTask.STARTING_CLUSTER)
    }

    @Test
    fun `should update timestamp when appending tasks`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.appendTasks(flinkCluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update timestamp when advancing tasks`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.selectNextTask(flinkCluster)
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update timestamp when resetting tasks`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.resetTasks(flinkCluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store savepoint path and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        assertThat(OperatorAnnotations.getSavepointTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.setSavepointPath(flinkCluster, "/tmp/xxx")
        assertThat(OperatorAnnotations.getSavepointPath(flinkCluster)).isEqualTo("/tmp/xxx")
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(OperatorAnnotations.getSavepointTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store savepoint request and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        assertThat(OperatorAnnotations.getSavepointTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.setSavepointRequest(flinkCluster, SavepointRequest("000", "XXX"))
        assertThat(OperatorAnnotations.getSavepointRequest(flinkCluster)).isEqualTo(SavepointRequest("000", "XXX"))
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(OperatorAnnotations.getSavepointTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should update savepoint timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        assertThat(OperatorAnnotations.getSavepointTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.updateSavepointTimestamp(flinkCluster)
        assertThat(OperatorAnnotations.getSavepointRequest(flinkCluster)).isNull()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(OperatorAnnotations.getSavepointTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store operator status and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.setTaskStatus(flinkCluster, TaskStatus.AWAITING)
        assertThat(OperatorAnnotations.getCurrentTaskStatus(flinkCluster)).isEqualTo(TaskStatus.AWAITING)
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store cluster status and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.setClusterStatus(flinkCluster, ClusterStatus.CHECKPOINTING)
        assertThat(OperatorAnnotations.getClusterStatus(flinkCluster)).isEqualTo(ClusterStatus.CHECKPOINTING)
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store job manager digest and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.setJobManagerDigest(flinkCluster, "XXX")
        assertThat(OperatorAnnotations.getJobManagerDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task manager digest and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.setTaskManagerDigest(flinkCluster, "XXX")
        assertThat(OperatorAnnotations.getTaskManagerDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store flink image digest and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.setFlinkImageDigest(flinkCluster, "XXX")
        assertThat(OperatorAnnotations.getFlinkImageDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store flink job digest and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.setFlinkJobDigest(flinkCluster, "XXX")
        assertThat(OperatorAnnotations.getFlinkJobDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task attempts and update timestamp`() {
        val timestamp = System.currentTimeMillis()
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isEqualTo(0)
        OperatorAnnotations.setOperatorTaskAttempts(flinkCluster, 2)
        assertThat(OperatorAnnotations.getOperatorTaskAttempts(flinkCluster)).isEqualTo(2)
        assertThat(OperatorAnnotations.getOperatorTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }
}
