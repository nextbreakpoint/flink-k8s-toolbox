package com.nextbreakpoint.model

data class JobManagerConfig(
    val image: String,
    val pullSecrets: String,
    val pullPolicy: String,
    val storage: StorageConfig,
    val resources: ResourcesConfig,
    val serviceMode: String
)