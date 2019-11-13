package com.nextbreakpoint.flinkoperator.common.model

enum class OperatorTask {
    InitialiseCluster,
    TerminatedCluster,
    SuspendCluster,
    ClusterHalted,
    ClusterRunning,
    StartingCluster,
    StoppingCluster,
    UpdatingCluster,
    RescaleCluster,
    CreatingSavepoint,
    TriggerSavepoint,
    EraseSavepoint,
    CreateResources,
    DeleteResources,
    ReplaceResources,
    CreateBootstrapJob,
    DeleteBootstrapJob,
    TerminatePods,
    RestartPods,
    CancelJob,
    StartJob,
    StopJob
}