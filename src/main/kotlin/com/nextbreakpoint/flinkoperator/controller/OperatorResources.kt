package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

data class OperatorResources(
    val jarUploadJobs: Map<ClusterId, V1Job>,
    val jobmanagerServices: Map<ClusterId, V1Service>,
    val jobmanagerStatefulSets: Map<ClusterId, V1StatefulSet>,
    val taskmanagerStatefulSets: Map<ClusterId, V1StatefulSet>,
    val jobmanagerPersistentVolumeClaims: Map<ClusterId, V1PersistentVolumeClaim>,
    val taskmanagerPersistentVolumeClaims: Map<ClusterId, V1PersistentVolumeClaim>
)