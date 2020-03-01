package com.nextbreakpoint.flinkoperator.controller.core

import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

data class CachedResources(
    val bootstrapJob: V1Job?,
    val jobmanagerService: V1Service?,
    val jobmanagerStatefulSet: V1StatefulSet?,
    val taskmanagerStatefulSet: V1StatefulSet?,
    val jobmanagerPVC: V1PersistentVolumeClaim?,
    val taskmanagerPVC: V1PersistentVolumeClaim?
)