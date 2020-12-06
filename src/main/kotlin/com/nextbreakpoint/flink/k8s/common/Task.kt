package com.nextbreakpoint.flink.k8s.common

abstract class Task<T> {
    abstract fun execute(target: T)
}