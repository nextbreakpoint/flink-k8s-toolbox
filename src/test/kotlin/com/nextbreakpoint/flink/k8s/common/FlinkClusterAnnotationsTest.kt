package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions
import org.joda.time.DateTime
import org.junit.jupiter.api.Test

class FlinkClusterAnnotationsTest {
    private val flinkCluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")

    @Test
    fun `cluster should store manual action`() {
        val timestamp1 = DateTime(System.currentTimeMillis())
        FlinkClusterAnnotations.setRequestedAction(flinkCluster, Action.START)
        Assertions.assertThat(FlinkClusterAnnotations.getRequestedAction(flinkCluster)).isEqualTo(Action.START)
        Assertions.assertThat(FlinkClusterAnnotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp1)
        val timestamp2 = DateTime(System.currentTimeMillis())
        FlinkClusterAnnotations.setRequestedAction(flinkCluster, Action.STOP)
        Assertions.assertThat(FlinkClusterAnnotations.getRequestedAction(flinkCluster)).isEqualTo(Action.STOP)
        Assertions.assertThat(FlinkClusterAnnotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp2)
    }

    @Test
    fun `cluster should store delete resources`() {
        val timestamp1 = DateTime(System.currentTimeMillis())
        FlinkClusterAnnotations.setDeleteResources(flinkCluster, true)
        Assertions.assertThat(FlinkClusterAnnotations.isDeleteResources(flinkCluster)).isTrue()
        Assertions.assertThat(FlinkClusterAnnotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp1)
        val timestamp2 = DateTime(System.currentTimeMillis())
        FlinkClusterAnnotations.setDeleteResources(flinkCluster, false)
        Assertions.assertThat(FlinkClusterAnnotations.isDeleteResources(flinkCluster)).isFalse()
        Assertions.assertThat(FlinkClusterAnnotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp2)
    }

    @Test
    fun `cluster should store without savepoint`() {
        val timestamp1 = DateTime(System.currentTimeMillis())
        FlinkClusterAnnotations.setWithoutSavepoint(flinkCluster, true)
        Assertions.assertThat(FlinkClusterAnnotations.isWithoutSavepoint(flinkCluster)).isTrue()
        Assertions.assertThat(FlinkClusterAnnotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp1)
        val timestamp2 = DateTime(System.currentTimeMillis())
        FlinkClusterAnnotations.setWithoutSavepoint(flinkCluster, false)
        Assertions.assertThat(FlinkClusterAnnotations.isWithoutSavepoint(flinkCluster)).isFalse()
        Assertions.assertThat(FlinkClusterAnnotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp2)
    }
}
