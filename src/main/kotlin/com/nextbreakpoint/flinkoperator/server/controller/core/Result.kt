package com.nextbreakpoint.flinkoperator.server.controller.core

data class Result<T>(val status: ResultStatus, val output: T) {
    fun isSuccessful() = status == ResultStatus.OK
}
