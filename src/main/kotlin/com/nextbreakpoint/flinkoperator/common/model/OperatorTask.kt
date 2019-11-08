package com.nextbreakpoint.flinkoperator.common.model

enum class OperatorTask {
    INITIALISE_CLUSTER,
    TERMINATE_CLUSTER,
    SUSPEND_CLUSTER,
    CLUSTER_HALTED,
    CLUSTER_RUNNING,
    STARTING_CLUSTER,
    STOPPING_CLUSTER,
    RESCALE_CLUSTER,
    CREATING_SAVEPOINT,
    CREATE_SAVEPOINT,
    ERASE_SAVEPOINT,
    CREATE_RESOURCES,
    DELETE_RESOURCES,
    DELETE_UPLOAD_JOB,
    TERMINATE_PODS,
    RESTART_PODS,
    UPLOAD_JAR,
    CANCEL_JOB,
    START_JOB,
    STOP_JOB
}