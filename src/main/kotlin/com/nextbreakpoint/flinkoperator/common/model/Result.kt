package com.nextbreakpoint.flinkoperator.common.model

data class Result<T>(val status: ResultStatus, val output: T) {
    fun isCompleted() = status == ResultStatus.SUCCESS
    fun isAwaiting() = status == ResultStatus.AWAIT
    fun isFailed() = status == ResultStatus.FAILED
}
