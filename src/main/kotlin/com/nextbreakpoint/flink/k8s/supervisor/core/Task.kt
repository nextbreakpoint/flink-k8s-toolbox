package com.nextbreakpoint.flink.k8s.supervisor.core

abstract class Task<T> {
    abstract fun execute(target: T)
}