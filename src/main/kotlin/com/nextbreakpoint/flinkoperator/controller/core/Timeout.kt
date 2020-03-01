package com.nextbreakpoint.flinkoperator.controller.core

// TODO parameterize?
object Timeout {
    val STARTING_JOB_TIMEOUT = 300L
    val STOPPING_JOB_TIMEOUT = 600L
    val CREATING_SAVEPOINT_TIMEOUT = 600L
}
