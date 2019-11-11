package com.nextbreakpoint.flinkoperator.controller.resources

import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

class ClusterResourcesTest {
    private val bootstrapJob = mock(V1Job::class.java)
    private val jobmanagerService = mock(V1Service::class.java)
    private val jobmanagerStatefulSet = mock(V1StatefulSet::class.java)
    private val taskmanagerStatefulSet = mock(V1StatefulSet::class.java)

    private val clusterResources = ClusterResources(
        bootstrapJob = bootstrapJob,
        jobmanagerService = jobmanagerService,
        jobmanagerStatefulSet = jobmanagerStatefulSet,
        taskmanagerStatefulSet = taskmanagerStatefulSet
    )

    @Test
    fun `should copy resources and modify bootstrap job`() {
        assertThat(clusterResources.bootstrapJob).isEqualTo(bootstrapJob)

        val resources = clusterResources.withBootstrapJob(null)

        assertThat(resources).isNotNull()
        assertThat(resources.bootstrapJob).isNull()
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)
    }

    @Test
    fun `should copy resources and modify job manager service`() {
        assertThat(clusterResources.jobmanagerService).isEqualTo(jobmanagerService)

        val resources = clusterResources.withJobManagerService(null)

        assertThat(resources).isNotNull()
        assertThat(resources.bootstrapJob).isEqualTo(bootstrapJob)
        assertThat(resources.jobmanagerService).isNull()
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)
    }

    @Test
    fun `should copy resources and modify job manager statefulset`() {
        assertThat(clusterResources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)

        val resources = clusterResources.withJobManagerStatefulSet(null)

        assertThat(resources).isNotNull()
        assertThat(resources.bootstrapJob).isEqualTo(bootstrapJob)
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.jobmanagerStatefulSet).isNull()
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)
    }

    @Test
    fun `should copy resources and modify task manager statefulset`() {
        assertThat(clusterResources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)

        val resources = clusterResources.withTaskManagerStatefulSet(null)

        assertThat(resources).isNotNull()
        assertThat(resources.bootstrapJob).isEqualTo(bootstrapJob)
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isNull()
    }
}