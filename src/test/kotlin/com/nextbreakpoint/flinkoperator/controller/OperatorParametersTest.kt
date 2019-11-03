package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkOperatorSpec
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class OperatorParametersTest {
    private val flinkCluster = TestFactory.aCluster(name = "test", namespace = "flink")

    @BeforeEach
    fun configure() {
        flinkCluster.spec.flinkOperator = V1FlinkOperatorSpec()
    }

    @Test
    fun `initially a cluster doesn't have a savepoint`() {
        assertThat(OperatorParameters.getSavepointPath(flinkCluster)).isNull()
    }

    @Test
    fun `the default mode mode is MANUAL`() {
        assertThat(OperatorParameters.getSavepointMode(flinkCluster)).isEqualTo("MANUAL")
    }

    @Test
    fun `the default savepoint interval is 1h`() {
        assertThat(OperatorParameters.getSavepointInterval(flinkCluster)).isEqualTo(36000L)
    }

    @Test
    fun `the default savepoint target path is null`() {
        assertThat(OperatorParameters.getSavepointTargetPath(flinkCluster)).isNull()
    }

    @Test
    fun `the default job restart policy is NEVER`() {
        assertThat(OperatorParameters.getJobRestartPolicy(flinkCluster)).isEqualTo("NEVER")
    }

    @Test
    fun `should return savepoint path`() {
        flinkCluster.spec.flinkOperator.savepointPath = "/tmp/000"
        assertThat(OperatorParameters.getSavepointPath(flinkCluster)).isEqualTo("/tmp/000")
    }

    @Test
    fun `should return savepoint target path`() {
        flinkCluster.spec.flinkOperator.savepointTargetPath = "/tmp"
        assertThat(OperatorParameters.getSavepointTargetPath(flinkCluster)).isEqualTo("/tmp")
    }

    @Test
    fun `should return savepoint interval`() {
        flinkCluster.spec.flinkOperator.savepointInterval = 60L
        assertThat(OperatorParameters.getSavepointInterval(flinkCluster)).isEqualTo(60L)
    }

    @Test
    fun `should return savepoint mode`() {
        flinkCluster.spec.flinkOperator.savepointMode = "AUTOMATIC"
        assertThat(OperatorParameters.getSavepointMode(flinkCluster)).isEqualTo("AUTOMATIC")
    }

    @Test
    fun `should return job restart policy`() {
        flinkCluster.spec.flinkOperator.jobRestartPolicy = "ONFAILURE"
        assertThat(OperatorParameters.getJobRestartPolicy(flinkCluster)).isEqualTo("ONFAILURE")
    }

    @Test
    fun `should remove quotes from savepoint path`() {
        flinkCluster.spec.flinkOperator.savepointPath = "\"/tmp/000\""
        assertThat(OperatorParameters.getSavepointPath(flinkCluster)).isEqualTo("/tmp/000")
    }
}
