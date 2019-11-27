package com.nextbreakpoint.flinkoperator.common.model

enum class ClusterTask {
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
    CreateBootstrapJob,
    DeleteBootstrapJob,
    TerminatePods,
    RestartPods,
    CancelJob,
    StartJob,
    StopJob
}