package com.nextbreakpoint.flinkoperator.controller.core

// TODO parameterize?
object Timeout {
    val DELETING_CLUSTER_TIMEOUT = 120L
    val CREATING_CLUSTER_TIMEOUT = 600L
    val RESCALING_CLUSTER_TIMEOUT = 120L
    val BOOTSTRAPPING_JOB_TIMEOUT = 300L
    val CANCELLING_JOB_TIMEOUT = 600L
    val STARTING_JOB_TIMEOUT = 300L
    val STOPPING_JOB_TIMEOUT = 600L
    val TERMINATING_RESOURCES_TIMEOUT = 120L
    val CREATING_SAVEPOINT_TIMEOUT = 600L
}
