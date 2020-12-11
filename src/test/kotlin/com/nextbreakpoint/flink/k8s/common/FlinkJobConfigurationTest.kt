package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.k8s.crd.V1RestartSpec
import com.nextbreakpoint.flink.k8s.crd.V1SavepointSpec
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class FlinkJobConfigurationTest {
    private val flinkJob = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")

    @BeforeEach
    fun configure() {
        flinkJob.spec.savepoint = V1SavepointSpec.builder().build()
        flinkJob.spec.restart = V1RestartSpec.builder().build()
    }

    @Test
    fun `initially a cluster doesn't have a savepoint`() {
        assertThat(FlinkJobConfiguration.getSavepointPath(flinkJob)).isNull()
    }

    @Test
    fun `the default mode mode is MANUAL`() {
        assertThat(FlinkJobConfiguration.getSavepointMode(flinkJob)).isEqualTo("MANUAL")
    }

    @Test
    fun `the default savepoint interval is 1h`() {
        assertThat(FlinkJobConfiguration.getSavepointInterval(flinkJob)).isEqualTo(3600L)
    }

    @Test
    fun `the default savepoint target path is null`() {
        assertThat(FlinkJobConfiguration.getSavepointTargetPath(flinkJob)).isNull()
    }

    @Test
    fun `the default restart policy is NEVER`() {
        assertThat(FlinkJobConfiguration.getRestartPolicy(flinkJob)).isEqualTo("NEVER")
    }

    @Test
    fun `should return savepoint path`() {
        flinkJob.spec.savepoint.savepointPath = "/tmp/000"
        assertThat(FlinkJobConfiguration.getSavepointPath(flinkJob)).isEqualTo("/tmp/000")
    }

    @Test
    fun `should return savepoint target path`() {
        flinkJob.spec.savepoint.savepointTargetPath = "/tmp"
        assertThat(FlinkJobConfiguration.getSavepointTargetPath(flinkJob)).isEqualTo("/tmp")
    }

    @Test
    fun `should return savepoint interval`() {
        flinkJob.spec.savepoint.savepointInterval = 60L
        assertThat(FlinkJobConfiguration.getSavepointInterval(flinkJob)).isEqualTo(60L)
    }

    @Test
    fun `should return savepoint mode`() {
        flinkJob.spec.savepoint.savepointMode = "AUTOMATIC"
        assertThat(FlinkJobConfiguration.getSavepointMode(flinkJob)).isEqualTo("AUTOMATIC")
    }

    @Test
    fun `should return restart policy`() {
        flinkJob.spec.restart.restartPolicy = "ALWAYS"
        assertThat(FlinkJobConfiguration.getRestartPolicy(flinkJob)).isEqualTo("ALWAYS")
    }

    @Test
    fun `should remove quotes from savepoint path`() {
        flinkJob.spec.savepoint.savepointPath = "\"/tmp/000\""
        assertThat(FlinkJobConfiguration.getSavepointPath(flinkJob)).isEqualTo("/tmp/000")
    }
}
