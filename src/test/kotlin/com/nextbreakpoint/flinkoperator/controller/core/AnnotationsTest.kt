package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions
import org.joda.time.DateTime
import org.junit.jupiter.api.Test

class AnnotationsTest {
    private val flinkCluster = TestFactory.aCluster(name = "test", namespace = "flink")

    @Test
    fun `cluster should store manual action`() {
        val timestamp1 = DateTime(System.currentTimeMillis())
        Annotations.setManualAction(flinkCluster, ManualAction.START)
        Assertions.assertThat(Annotations.getManualAction(flinkCluster)).isEqualTo(ManualAction.START)
        Assertions.assertThat(Annotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp1)
        val timestamp2 = DateTime(System.currentTimeMillis())
        Annotations.setManualAction(flinkCluster, ManualAction.STOP)
        Assertions.assertThat(Annotations.getManualAction(flinkCluster)).isEqualTo(ManualAction.STOP)
        Assertions.assertThat(Annotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp2)
    }

    @Test
    fun `cluster should store delete resources`() {
        val timestamp1 = DateTime(System.currentTimeMillis())
        Annotations.setDeleteResources(flinkCluster, true)
        Assertions.assertThat(Annotations.isDeleteResources(flinkCluster)).isTrue()
        Assertions.assertThat(Annotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp1)
        val timestamp2 = DateTime(System.currentTimeMillis())
        Annotations.setDeleteResources(flinkCluster, false)
        Assertions.assertThat(Annotations.isDeleteResources(flinkCluster)).isFalse()
        Assertions.assertThat(Annotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp2)
    }

    @Test
    fun `cluster should store without savepoint`() {
        val timestamp1 = DateTime(System.currentTimeMillis())
        Annotations.setWithoutSavepoint(flinkCluster, true)
        Assertions.assertThat(Annotations.isWithoutSavepoint(flinkCluster)).isTrue()
        Assertions.assertThat(Annotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp1)
        val timestamp2 = DateTime(System.currentTimeMillis())
        Annotations.setWithoutSavepoint(flinkCluster, false)
        Assertions.assertThat(Annotations.isWithoutSavepoint(flinkCluster)).isFalse()
        Assertions.assertThat(Annotations.getActionTimestamp(flinkCluster)).isGreaterThanOrEqualTo(timestamp2)
    }
}
