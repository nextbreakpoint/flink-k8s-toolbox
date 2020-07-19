package com.nextbreakpoint.flinkoperator.controller.core

data class OperationResult<T>(val status: OperationStatus, val output: T) {
    fun isSuccessful() = status == OperationStatus.OK
}
