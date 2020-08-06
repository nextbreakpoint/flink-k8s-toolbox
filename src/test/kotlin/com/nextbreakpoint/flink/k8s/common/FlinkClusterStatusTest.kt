package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.joda.time.DateTime
import org.junit.jupiter.api.Test

class FlinkClusterStatusTest {
    private val flinkCluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")

    @Test
    fun `should store cluster status and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isEqualTo(DateTime(0))
        assertThat(FlinkClusterStatus.getSupervisorStatus(flinkCluster)).isEqualTo(ClusterStatus.Unknown)
        FlinkClusterStatus.setSupervisorStatus(flinkCluster, ClusterStatus.Stopping)
        assertThat(FlinkClusterStatus.getSupervisorStatus(flinkCluster)).isEqualTo(ClusterStatus.Stopping)
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store job manager digest and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isEqualTo(DateTime(0))
        assertThat(FlinkClusterStatus.getJobManagerDigest(flinkCluster)).isNull()
        FlinkClusterStatus.setJobManagerDigest(flinkCluster, "XXX")
        assertThat(FlinkClusterStatus.getJobManagerDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task manager digest and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isEqualTo(DateTime(0))
        assertThat(FlinkClusterStatus.getTaskManagerDigest(flinkCluster)).isNull()
        FlinkClusterStatus.setTaskManagerDigest(flinkCluster, "XXX")
        assertThat(FlinkClusterStatus.getTaskManagerDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store flink image digest and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isEqualTo(DateTime(0))
        assertThat(FlinkClusterStatus.getRuntimeDigest(flinkCluster)).isNull()
        FlinkClusterStatus.setRuntimeDigest(flinkCluster, "XXX")
        assertThat(FlinkClusterStatus.getRuntimeDigest(flinkCluster)).isEqualTo("XXX")
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task managers and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isEqualTo(DateTime(0))
        assertThat(FlinkClusterStatus.getTaskManagers(flinkCluster)).isEqualTo(0)
        FlinkClusterStatus.setTaskManagers(flinkCluster, 2)
        assertThat(FlinkClusterStatus.getTaskManagers(flinkCluster)).isEqualTo(2)
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store active task managers and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isEqualTo(DateTime(0))
        assertThat(FlinkClusterStatus.getActiveTaskManagers(flinkCluster)).isEqualTo(0)
        FlinkClusterStatus.setActiveTaskManagers(flinkCluster, 2)
        assertThat(FlinkClusterStatus.getActiveTaskManagers(flinkCluster)).isEqualTo(2)
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store task slots and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isEqualTo(DateTime(0))
        assertThat(FlinkClusterStatus.getTaskSlots(flinkCluster)).isEqualTo(0)
        FlinkClusterStatus.setTaskSlots(flinkCluster, 2)
        assertThat(FlinkClusterStatus.getTaskSlots(flinkCluster)).isEqualTo(2)
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store total task slots and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isEqualTo(DateTime(0))
        assertThat(FlinkClusterStatus.getTotalTaskSlots(flinkCluster)).isEqualTo(0)
        FlinkClusterStatus.setTotalTaskSlots(flinkCluster, 4)
        assertThat(FlinkClusterStatus.getTotalTaskSlots(flinkCluster)).isEqualTo(4)
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store label selector and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isEqualTo(DateTime(0))
        assertThat(FlinkClusterStatus.getLabelSelector(flinkCluster)).isNull()
        FlinkClusterStatus.setLabelSelector(flinkCluster, "xxxx")
        assertThat(FlinkClusterStatus.getLabelSelector(flinkCluster)).isEqualTo("xxxx")
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }

    @Test
    fun `should store service mode and update timestamp`() {
        val timestamp = DateTime(System.currentTimeMillis())
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isEqualTo(DateTime(0))
        assertThat(FlinkClusterStatus.getServiceMode(flinkCluster)).isNull()
        FlinkClusterStatus.setServiceMode(flinkCluster, "NodePort")
        assertThat(FlinkClusterStatus.getServiceMode(flinkCluster)).isEqualTo("NodePort")
        assertThat(FlinkClusterStatus.getStatusTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp)
    }
}
