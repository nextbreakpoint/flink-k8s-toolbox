package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesBuilder
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesFactory
import com.nextbreakpoint.flinkoperator.controller.testing.TestFactory
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import java.util.UUID

@RunWith(JUnitPlatform::class)
class ClusterResourcesBuilderTest {
    private val factory = mock(ClusterResourcesFactory::class.java)

    private val jarUploadJob = mock(V1Job::class.java)
    private val jobmanagerService = mock(V1Service::class.java)
    private val jobmanagerStatefulSet = mock(V1StatefulSet::class.java)
    private val taskmanagerStatefulSet = mock(V1StatefulSet::class.java)

    private val cluster = TestFactory.aCluster()

    private val clusterId = UUID.randomUUID().toString()

    private val builder = ClusterResourcesBuilder(
        factory,
        "test",
        clusterId,
        "myself",
        cluster
    )

    private fun <T> any(): T {
        Mockito.any<T>()
        return uninitialized()
    }

    private fun <T> eq(value : T): T {
        Mockito.eq<T>(value)
        return uninitialized()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> uninitialized(): T = null as T

    @BeforeEach
    fun setup() {
        `when`(factory.createJarUploadJob(any(), any(), any(), any())).thenReturn(jarUploadJob)
        `when`(factory.createJobManagerService(any(), any(), any(), any())).thenReturn(jobmanagerService)
        `when`(factory.createJobManagerStatefulSet(any(), any(), any(), any())).thenReturn(jobmanagerStatefulSet)
        `when`(factory.createTaskManagerStatefulSet(any(), any(), any(), any())).thenReturn(taskmanagerStatefulSet)
    }

    @Test
    fun `should invoke factory to create resources`() {
        val resources = builder.build()

        assertThat(resources).isNotNull()
        assertThat(resources.jarUploadJob).isEqualTo(jarUploadJob)
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)

        verify(factory, times(1)).createJarUploadJob(eq("test"), eq(clusterId), eq("myself"), eq(cluster))
        verify(factory, times(1)).createJobManagerService(eq("test"), eq(clusterId), eq("myself"), eq(cluster))
        verify(factory, times(1)).createJobManagerStatefulSet(eq("test"), eq(clusterId), eq("myself"), eq(cluster))
        verify(factory, times(1)).createTaskManagerStatefulSet(eq("test"), eq(clusterId), eq("myself"), eq(cluster))
    }

    @Test
    fun `should copy resources and modify jobmanager service`() {
        val resources = builder.build().withJobManagerService(null)

        assertThat(resources).isNotNull()
        assertThat(resources.jarUploadJob).isEqualTo(jarUploadJob)
        assertThat(resources.jobmanagerService).isNull()
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)
    }

    @Test
    fun `should copy resources and modify jar upload job`() {
        val resources = builder.build().withJarUploadJob(null)

        assertThat(resources).isNotNull()
        assertThat(resources.jarUploadJob).isNull()
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)
    }

    @Test
    fun `should copy resources and modify jobmanager statefulset`() {
        val resources = builder.build().withJobManagerStatefulSet(null)

        assertThat(resources).isNotNull()
        assertThat(resources.jarUploadJob).isEqualTo(jarUploadJob)
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.jobmanagerStatefulSet).isNull()
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)
    }

    @Test
    fun `should copy resources and modify taskmanager statefulset`() {
        val resources = builder.build().withTaskManagerStatefulSet(null)

        assertThat(resources).isNotNull()
        assertThat(resources.jarUploadJob).isEqualTo(jarUploadJob)
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isNull()
    }
}