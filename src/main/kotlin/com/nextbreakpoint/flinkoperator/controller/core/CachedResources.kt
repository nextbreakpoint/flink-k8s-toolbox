package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

data class CachedResources(
    val bootstrapJobs: Map<ClusterId, V1Job> = mapOf(),
    val jobmanagerServices: Map<ClusterId, V1Service> = mapOf(),
    val jobmanagerStatefulSets: Map<ClusterId, V1StatefulSet> = mapOf(),
    val taskmanagerStatefulSets: Map<ClusterId, V1StatefulSet> = mapOf(),
    val jobmanagerPersistentVolumeClaims: Map<ClusterId, V1PersistentVolumeClaim> = mapOf(),
    val taskmanagerPersistentVolumeClaims: Map<ClusterId, V1PersistentVolumeClaim> = mapOf()
)