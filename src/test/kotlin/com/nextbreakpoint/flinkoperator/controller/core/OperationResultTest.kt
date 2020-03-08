package com.nextbreakpoint.flinkoperator.controller.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class OperationResultTest {
    @Test
    fun `should return true when completed otherwise false`() {
        val result = OperationResult<Any>(status = OperationStatus.COMPLETED, output = "123")
        assertThat(result.isCompleted()).isTrue()
        assertThat(result.isFailed()).isFalse()
        assertThat(result.isRetry()).isFalse()
    }

    @Test
    fun `should return true when failed otherwise false`() {
        val result = OperationResult<Any>(status = OperationStatus.FAILED, output = "123")
        assertThat(result.isCompleted()).isFalse()
        assertThat(result.isFailed()).isTrue()
        assertThat(result.isRetry()).isFalse()
    }

    @Test
    fun `should return true when retry otherwise false`() {
        val result = OperationResult<Any>(status = OperationStatus.RETRY, output = "123")
        assertThat(result.isCompleted()).isFalse()
        assertThat(result.isFailed()).isFalse()
        assertThat(result.isRetry()).isTrue()
    }
}
