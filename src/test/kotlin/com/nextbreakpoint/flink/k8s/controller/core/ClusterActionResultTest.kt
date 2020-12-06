package com.nextbreakpoint.flink.k8s.controller.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ClusterActionResultTest {
    @Test
    fun `should return true when completed otherwise false`() {
        val result = Result<Any>(status = ResultStatus.OK, output = "123")
        assertThat(result.isSuccessful()).isTrue()
    }
}
