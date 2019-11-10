package com.nextbreakpoint.flinkoperator.common.model

enum class OperatorTask {
    InitialiseCluster,
    TerminatedCluster,
    SuspendCluster,
    ClusterHalted,
    ClusterRunning,
    StartingCluster,
    StoppingCluster,
    RescaleCluster,
    CreatingSavepoint,
    TriggerSavepoint,
    EraseSavepoint,
    CreateResources,
    DeleteResources,
    CreateBootstrapJob,
    DeleteBootstrapJob,
    TerminatePods,
    RestartPods,
    CancelJob,
    StartJob,
    StopJob
}