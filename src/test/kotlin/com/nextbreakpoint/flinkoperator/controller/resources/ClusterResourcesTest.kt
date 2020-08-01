package com.nextbreakpoint.flinkoperator.controller.resources

import io.kubernetes.client.models.V1Pod
import io.kubernetes.client.models.V1Service
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

class ClusterResourcesTest {
    private val service = mock(V1Service::class.java)
    private val jobmanagerPod = mock(V1Pod::class.java)
    private val taskmanagerPod = mock(V1Pod::class.java)

    private val clusterResources = ClusterResources(
        service = service,
        jobmanagerPod = jobmanagerPod,
        taskmanagerPod = taskmanagerPod
    )

    @Test
    fun `should copy resources and modify job manager service`() {
        assertThat(clusterResources.service).isEqualTo(service)

        val resources = clusterResources.withService(null)

        assertThat(resources).isNotNull()
        assertThat(resources.service).isNull()
        assertThat(resources.jobmanagerPod).isEqualTo(jobmanagerPod)
        assertThat(resources.taskmanagerPod).isEqualTo(taskmanagerPod)
    }

    @Test
    fun `should copy resources and modify job manager pod`() {
        assertThat(clusterResources.jobmanagerPod).isEqualTo(jobmanagerPod)

        val resources = clusterResources.withJobManagerPod(null)

        assertThat(resources).isNotNull()
        assertThat(resources.service).isEqualTo(service)
        assertThat(resources.jobmanagerPod).isNull()
        assertThat(resources.taskmanagerPod).isEqualTo(taskmanagerPod)
    }

    @Test
    fun `should copy resources and modify task manager pod`() {
        assertThat(clusterResources.taskmanagerPod).isEqualTo(taskmanagerPod)

        val resources = clusterResources.withTaskManagerPod(null)

        assertThat(resources).isNotNull()
        assertThat(resources.service).isEqualTo(service)
        assertThat(resources.jobmanagerPod).isEqualTo(jobmanagerPod)
        assertThat(resources.taskmanagerPod).isNull()
    }
}