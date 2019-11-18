package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class CacheTest {
    private val cache = Cache()

    @Test
    fun `cache is empty`() {
        assertThat(cache.getFlinkClusters()).isEmpty()
    }

    @Test
    fun `should throw RuntimeException when looking for non existent cluster`() {
        assertThatThrownBy { cache.getFlinkCluster(ClusterId(namespace = "flink", name = "test", uuid = "123")) }.isInstanceOf(RuntimeException::class.java)
    }

    @Test
    fun `should throw RuntimeException when looking for non existent cluster id`() {
        assertThatThrownBy { cache.getClusterId(namespace = "flink", name = "test") }.isInstanceOf(RuntimeException::class.java)
    }

    @Test
    fun `should return empty list of clusters initially`() {
        assertThat(cache.getClusters()).isEmpty()
    }

    @Test
    fun `should return empty list of orphaned clusters initially`() {
        assertThat(cache.getOrphanedClusters()).isEmpty()
    }

    @Test
    fun `should update clusters when a cluster changed`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        assertThat(cache.getClusters()).isNotEmpty()
    }

    @Test
    fun `should find cluster when a cluster changed`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        assertThat(cache.getFlinkCluster(ClusterId(namespace = cluster.metadata.namespace, name = cluster.metadata.name, uuid = "123"))).isNotNull()
    }

    @Test
    fun `should find cluster id when a cluster changed`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        assertThat(cache.getClusterId(namespace = cluster.metadata.namespace, name = cluster.metadata.name)).isNotNull()
    }

    @Test
    fun `should return non empty list of clusters when a cluster changed`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        val clusters = cache.getClusters()
        assertThat(clusters).hasSize(2)
        assertThat(clusters.get(0).first.metadata.uid).isEqualTo("123")
        assertThat(clusters.get(1).first.metadata.uid).isEqualTo("456")
    }

    @Test
    fun `should update clusters when a cluster is deleted`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        cache.onFlinkClusterDeleted(cluster1)
        val clusters = cache.getClusters()
        assertThat(clusters).hasSize(1)
        assertThat(clusters.get(0).first.metadata.uid).isEqualTo("456")
    }

    @Test
    fun `should remove all clusters when all clusters are deleted`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        assertThat(cache.getClusters()).hasSize(2)
        cache.onFlinkClusterDeleteAll()
        assertThat(cache.getClusters()).hasSize(0)
    }

    @Test
    fun `should throw IllegalStateException when cluster is missing name`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cluster.metadata.name = null
        assertThatThrownBy { cache.onFlinkClusterChanged(cluster) }.isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `should throw IllegalStateException when cluster is missing uid`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = null
        assertThatThrownBy { cache.onFlinkClusterChanged(cluster) }.isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `should return a cluster with empty resources initially`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val clusters = cache.getClusters()
        assertThat(clusters.get(0).second.bootstrapJobs).isEmpty()
        assertThat(clusters.get(0).second.jobmanagerServices).isEmpty()
        assertThat(clusters.get(0).second.jobmanagerStatefulSets).isEmpty()
        assertThat(clusters.get(0).second.taskmanagerStatefulSets).isEmpty()
        assertThat(clusters.get(0).second.jobmanagerPersistentVolumeClaims).isEmpty()
        assertThat(clusters.get(0).second.taskmanagerPersistentVolumeClaims).isEmpty()
    }

    @Test
    fun `should return resources when resources are changed`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val bootstrapJob = TestFactory.aUploadJob(cluster)
        val jobManagerService1 = TestFactory.aJobManagerService(cluster)
        val jobManagerStatefulSet1 = TestFactory.aJobManagerStatefulSet(cluster)
        val taskManagerStatefulSet1 = TestFactory.aTaskManagerStatefulSet(cluster)
        val jobManagerPersistenVolumeClaim1 = TestFactory.aJobManagerPersistenVolumeClaim(cluster)
        val taskManagerPersistenVolumeClaim1 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster)
        cache.onJobChanged(bootstrapJob)
        cache.onServiceChanged(jobManagerService1)
        cache.onStatefulSetChanged(jobManagerStatefulSet1)
        cache.onStatefulSetChanged(taskManagerStatefulSet1)
        cache.onPersistentVolumeClaimChanged(jobManagerPersistenVolumeClaim1)
        cache.onPersistentVolumeClaimChanged(taskManagerPersistenVolumeClaim1)
        val resources = cache.getResources()
        assertThat(resources.bootstrapJobs).hasSize(1)
        assertThat(resources.jobmanagerServices).hasSize(1)
        assertThat(resources.jobmanagerStatefulSets).hasSize(1)
        assertThat(resources.taskmanagerStatefulSets).hasSize(1)
        assertThat(resources.jobmanagerPersistentVolumeClaims).hasSize(1)
        assertThat(resources.taskmanagerPersistentVolumeClaims).hasSize(1)
    }

    @Test
    fun `should update resources when resources are changed`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val bootstrapJob1 = TestFactory.aUploadJob(cluster1)
        val jobManagerService1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerStatefulSet1 = TestFactory.aJobManagerStatefulSet(cluster1)
        val taskManagerStatefulSet1 = TestFactory.aTaskManagerStatefulSet(cluster1)
        val jobManagerPersistenVolumeClaim1 = TestFactory.aJobManagerPersistenVolumeClaim(cluster1)
        val taskManagerPersistenVolumeClaim1 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val bootstrapJob2 = TestFactory.aUploadJob(cluster2)
        val jobManagerService2 = TestFactory.aJobManagerService(cluster2)
        val jobManagerStatefulSet2 = TestFactory.aJobManagerStatefulSet(cluster2)
        val taskManagerStatefulSet2 = TestFactory.aTaskManagerStatefulSet(cluster2)
        val jobManagerPersistenVolumeClaim2 = TestFactory.aJobManagerPersistenVolumeClaim(cluster2)
        val taskManagerPersistenVolumeClaim2 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster2)
        cache.onJobChanged(bootstrapJob1)
        cache.onServiceChanged(jobManagerService1)
        cache.onStatefulSetChanged(jobManagerStatefulSet1)
        cache.onStatefulSetChanged(taskManagerStatefulSet1)
        cache.onPersistentVolumeClaimChanged(jobManagerPersistenVolumeClaim1)
        cache.onPersistentVolumeClaimChanged(taskManagerPersistenVolumeClaim1)
        cache.onJobChanged(bootstrapJob2)
        cache.onServiceChanged(jobManagerService2)
        cache.onStatefulSetChanged(jobManagerStatefulSet2)
        cache.onStatefulSetChanged(taskManagerStatefulSet2)
        cache.onPersistentVolumeClaimChanged(jobManagerPersistenVolumeClaim2)
        cache.onPersistentVolumeClaimChanged(taskManagerPersistenVolumeClaim2)
        val clusters = cache.getClusters()
        assertThat(clusters.get(0).second.bootstrapJobs).hasSize(2)
        assertThat(clusters.get(0).second.jobmanagerServices).hasSize(2)
        assertThat(clusters.get(0).second.jobmanagerStatefulSets).hasSize(2)
        assertThat(clusters.get(0).second.taskmanagerStatefulSets).hasSize(2)
        assertThat(clusters.get(0).second.jobmanagerPersistentVolumeClaims).hasSize(2)
        assertThat(clusters.get(0).second.taskmanagerPersistentVolumeClaims).hasSize(2)
    }

    @Test
    fun `should update resources when resources are deleted`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val bootstrapJob1 = TestFactory.aUploadJob(cluster1)
        val jobManagerService1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerStatefulSet1 = TestFactory.aJobManagerStatefulSet(cluster1)
        val taskManagerStatefulSet1 = TestFactory.aTaskManagerStatefulSet(cluster1)
        val jobManagerPersistenVolumeClaim1 = TestFactory.aJobManagerPersistenVolumeClaim(cluster1)
        val taskManagerPersistenVolumeClaim1 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val bootstrapJob2 = TestFactory.aUploadJob(cluster2)
        val jobManagerService2 = TestFactory.aJobManagerService(cluster2)
        val jobManagerStatefulSet2 = TestFactory.aJobManagerStatefulSet(cluster2)
        val taskManagerStatefulSet2 = TestFactory.aTaskManagerStatefulSet(cluster2)
        val jobManagerPersistenVolumeClaim2 = TestFactory.aJobManagerPersistenVolumeClaim(cluster2)
        val taskManagerPersistenVolumeClaim2 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster2)
        cache.onJobChanged(bootstrapJob1)
        cache.onServiceChanged(jobManagerService1)
        cache.onStatefulSetChanged(jobManagerStatefulSet1)
        cache.onStatefulSetChanged(taskManagerStatefulSet1)
        cache.onPersistentVolumeClaimChanged(jobManagerPersistenVolumeClaim1)
        cache.onPersistentVolumeClaimChanged(taskManagerPersistenVolumeClaim1)
        cache.onJobChanged(bootstrapJob2)
        cache.onServiceChanged(jobManagerService2)
        cache.onStatefulSetChanged(jobManagerStatefulSet2)
        cache.onStatefulSetChanged(taskManagerStatefulSet2)
        cache.onPersistentVolumeClaimChanged(jobManagerPersistenVolumeClaim2)
        cache.onPersistentVolumeClaimChanged(taskManagerPersistenVolumeClaim2)
        cache.onJobDeleted(bootstrapJob1)
        cache.onServiceDeleted(jobManagerService1)
        cache.onStatefulSetDeleted(jobManagerStatefulSet1)
        cache.onStatefulSetDeleted(taskManagerStatefulSet1)
        cache.onPersistentVolumeClaimDeleted(jobManagerPersistenVolumeClaim1)
        cache.onPersistentVolumeClaimDeleted(taskManagerPersistenVolumeClaim1)
        val clusters = cache.getClusters()
        assertThat(clusters.get(0).second.bootstrapJobs).hasSize(1)
        assertThat(clusters.get(0).second.jobmanagerServices).hasSize(1)
        assertThat(clusters.get(0).second.jobmanagerStatefulSets).hasSize(1)
        assertThat(clusters.get(0).second.taskmanagerStatefulSets).hasSize(1)
        assertThat(clusters.get(0).second.jobmanagerPersistentVolumeClaims).hasSize(1)
        assertThat(clusters.get(0).second.taskmanagerPersistentVolumeClaims).hasSize(1)
    }

    @Test
    fun `should update resources when all resources are deleted`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val bootstrapJob1 = TestFactory.aUploadJob(cluster1)
        val jobManagerService1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerStatefulSet1 = TestFactory.aJobManagerStatefulSet(cluster1)
        val taskManagerStatefulSet1 = TestFactory.aTaskManagerStatefulSet(cluster1)
        val jobManagerPersistenVolumeClaim1 = TestFactory.aJobManagerPersistenVolumeClaim(cluster1)
        val taskManagerPersistenVolumeClaim1 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val bootstrapJob2 = TestFactory.aUploadJob(cluster2)
        val jobManagerService2 = TestFactory.aJobManagerService(cluster2)
        val jobManagerStatefulSet2 = TestFactory.aJobManagerStatefulSet(cluster2)
        val taskManagerStatefulSet2 = TestFactory.aTaskManagerStatefulSet(cluster2)
        val jobManagerPersistenVolumeClaim2 = TestFactory.aJobManagerPersistenVolumeClaim(cluster2)
        val taskManagerPersistenVolumeClaim2 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster2)
        cache.onJobChanged(bootstrapJob1)
        cache.onServiceChanged(jobManagerService1)
        cache.onStatefulSetChanged(jobManagerStatefulSet1)
        cache.onStatefulSetChanged(taskManagerStatefulSet1)
        cache.onPersistentVolumeClaimChanged(jobManagerPersistenVolumeClaim1)
        cache.onPersistentVolumeClaimChanged(taskManagerPersistenVolumeClaim1)
        cache.onJobChanged(bootstrapJob2)
        cache.onServiceChanged(jobManagerService2)
        cache.onStatefulSetChanged(jobManagerStatefulSet2)
        cache.onStatefulSetChanged(taskManagerStatefulSet2)
        cache.onPersistentVolumeClaimChanged(jobManagerPersistenVolumeClaim2)
        cache.onPersistentVolumeClaimChanged(taskManagerPersistenVolumeClaim2)
        cache.onJobDeleteAll()
        cache.onServiceDeleteAll()
        cache.onStatefulSetDeleteAll()
        cache.onStatefulSetDeleteAll()
        cache.onPersistentVolumeClaimDeleteAll()
        cache.onPersistentVolumeClaimDeleteAll()
        val clusters = cache.getClusters()
        assertThat(clusters.get(0).second.bootstrapJobs).isEmpty()
        assertThat(clusters.get(0).second.jobmanagerServices).isEmpty()
        assertThat(clusters.get(0).second.jobmanagerStatefulSets).isEmpty()
        assertThat(clusters.get(0).second.taskmanagerStatefulSets).isEmpty()
        assertThat(clusters.get(0).second.jobmanagerPersistentVolumeClaims).isEmpty()
        assertThat(clusters.get(0).second.taskmanagerPersistentVolumeClaims).isEmpty()
    }
}
