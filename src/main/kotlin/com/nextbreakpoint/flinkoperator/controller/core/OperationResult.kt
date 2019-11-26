package com.nextbreakpoint.flinkoperator.controller.core

data class OperationResult<T>(val status: OperationStatus, val output: T) {
    fun isCompleted() = status == OperationStatus.COMPLETED
    fun isFailed() = status == OperationStatus.FAILED
    fun isRetry() = status == OperationStatus.RETRY
}
