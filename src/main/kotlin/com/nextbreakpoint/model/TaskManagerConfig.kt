package com.nextbreakpoint.model

data class TaskManagerConfig(
    val image: String,
    val pullSecrets: String?,
    val pullPolicy: String,
    val serviceAccount: String,
    val taskSlots: Int,
    val replicas: Int,
    val storage: StorageConfig,
    val resources: ResourcesConfig
)