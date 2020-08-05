package com.nextbreakpoint.flinkoperator.server.common

import com.nextbreakpoint.flinkoperator.common.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.SavepointRequest
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.joda.time.DateTime
import org.junit.jupiter.api.Test

class StatusTest {
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")

    @Test
    fun `should store savepoint path and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getSavepointRequestTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getSavepointTimestamp(cluster)).isEqualTo(DateTime(0))
        Status.setSavepointPath(cluster, "/tmp/xxx")
        assertThat(Status.getSavepointPath(cluster)).isEqualTo("/tmp/xxx")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(Status.getSavepointRequestTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(Status.getSavepointTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should not store savepoint path and update timestamp if path is blank`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getSavepointRequestTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getSavepointTimestamp(cluster)).isEqualTo(DateTime(0))
        Status.setSavepointPath(cluster, "")
        assertThat(Status.getSavepointPath(cluster)).isNull()
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(Status.getSavepointRequestTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(Status.getSavepointTimestamp(cluster)).isEqualTo(DateTime(0))
    }

    @Test
    fun `should store savepoint request and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getSavepointRequestTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getSavepointRequest(cluster)).isNull()
        Status.setSavepointRequest(cluster, SavepointRequest("000", "XXX"))
        assertThat(Status.getSavepointRequest(cluster)).isEqualTo(SavepointRequest("000", "XXX"))
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(Status.getSavepointRequestTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should reset savepoint request and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getSavepointRequestTimestamp(cluster)).isEqualTo(DateTime(0))
        Status.resetSavepointRequest(cluster)
        assertThat(Status.getSavepointRequest(cluster)).isNull()
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
        assertThat(Status.getSavepointRequestTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store cluster status and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Unknown)
        Status.setClusterStatus(cluster, ClusterStatus.Cancelling)
        assertThat(Status.getClusterStatus(cluster)).isEqualTo(ClusterStatus.Cancelling)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store job manager digest and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getJobManagerDigest(cluster)).isNull()
        Status.setJobManagerDigest(cluster, "XXX")
        assertThat(Status.getJobManagerDigest(cluster)).isEqualTo("XXX")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task manager digest and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getTaskManagerDigest(cluster)).isNull()
        Status.setTaskManagerDigest(cluster, "XXX")
        assertThat(Status.getTaskManagerDigest(cluster)).isEqualTo("XXX")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store flink image digest and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getRuntimeDigest(cluster)).isNull()
        Status.setRuntimeDigest(cluster, "XXX")
        assertThat(Status.getRuntimeDigest(cluster)).isEqualTo("XXX")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store flink job digest and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getBootstrapDigest(cluster)).isNull()
        Status.setBootstrapDigest(cluster, "XXX")
        assertThat(Status.getBootstrapDigest(cluster)).isEqualTo("XXX")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task managers and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(0)
        Status.setTaskManagers(cluster, 2)
        assertThat(Status.getTaskManagers(cluster)).isEqualTo(2)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store active task managers and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getActiveTaskManagers(cluster)).isEqualTo(0)
        Status.setActiveTaskManagers(cluster, 2)
        assertThat(Status.getActiveTaskManagers(cluster)).isEqualTo(2)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task slots and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(0)
        Status.setTaskSlots(cluster, 2)
        assertThat(Status.getTaskSlots(cluster)).isEqualTo(2)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store total task slots and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getTotalTaskSlots(cluster)).isEqualTo(0)
        Status.setTotalTaskSlots(cluster, 4)
        assertThat(Status.getTotalTaskSlots(cluster)).isEqualTo(4)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store job parallelism and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getJobParallelism(cluster)).isEqualTo(0)
        Status.setJobParallelism(cluster, 4)
        assertThat(Status.getJobParallelism(cluster)).isEqualTo(4)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store label selector and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getLabelSelector(cluster)).isNull()
        Status.setLabelSelector(cluster, "xxxx")
        assertThat(Status.getLabelSelector(cluster)).isEqualTo("xxxx")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store savepoint mode and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getSavepointMode(cluster)).isNull()
        Status.setSavepointMode(cluster, "Manual")
        assertThat(Status.getSavepointMode(cluster)).isEqualTo("Manual")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store service mode and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getServiceMode(cluster)).isNull()
        Status.setServiceMode(cluster, "NodePort")
        assertThat(Status.getServiceMode(cluster)).isEqualTo("NodePort")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store job restart policy and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getRestartPolicy(cluster)).isNull()
        Status.setRestartPolicy(cluster, "Never")
        assertThat(Status.getRestartPolicy(cluster)).isEqualTo("Never")
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store bootstrap and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(Status.getStatusTimestamp(cluster)).isEqualTo(DateTime(0))
        assertThat(Status.getBootstrap(cluster)).isNull()
        Status.setBootstrap(cluster, cluster.spec.bootstrap)
        assertThat(Status.getBootstrap(cluster)).isNotNull()
        assertThat(Status.getBootstrap(cluster)).isEqualTo(cluster.spec.bootstrap)
        assertThat(Status.getStatusTimestamp(cluster)).isGreaterThanOrEqualTo(timestamp)
    }
}
