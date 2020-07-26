package com.nextbreakpoint.flinkoperator.controller.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class OperationResultTest {
    @Test
    fun `should return true when completed otherwise false`() {
        val result = OperationResult<Any>(status = OperationStatus.OK, output = "123")
        assertThat(result.isSuccessful()).isTrue()
    }
}
