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
        assertThat(cache.getFlinkClusters()).isEmpty()
    }

    @Test
    fun `should return empty list of cluster ids initially`() {
        assertThat(cache.getClusterIds()).isEmpty()
    }

    @Test
    fun `should update clusters when a cluster changed`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        assertThat(cache.getFlinkClusters()).isNotEmpty()
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
        val clusters = cache.getFlinkClusters()
        assertThat(clusters).hasSize(2)
        assertThat(clusters).containsExactlyInAnyOrder(cluster1, cluster2)
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
        val clusters = cache.getFlinkClusters()
        assertThat(clusters).hasSize(1)
        assertThat(clusters).containsExactlyInAnyOrder(cluster2)
    }

    @Test
    fun `should remove all clusters when all clusters are deleted`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        assertThat(cache.getFlinkClusters()).hasSize(2)
        cache.onFlinkClusterDeletedAll()
        assertThat(cache.getFlinkClusters()).hasSize(0)
    }

    @Test
    fun `should return non empty list of cluster ids when a cluster changed`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        val clusterIds = cache.getClusterIds()
        assertThat(clusterIds).hasSize(2)
        val clusterId1 = ClusterId(namespace = "flink", name = "test", uuid = "123")
        val clusterId2 = ClusterId(namespace = "flink", name = "test", uuid = "456")
        assertThat(clusterIds).containsExactlyInAnyOrder(clusterId1, clusterId2)
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
    fun `should return empty resources initially`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
        val resources = cache.getCachedResources(clusterId = clusterId)
        assertThat(resources.bootstrapJob).isNull()
        assertThat(resources.jobmanagerService).isNull()
        assertThat(resources.jobmanagerStatefulSet).isNull()
        assertThat(resources.taskmanagerStatefulSet).isNull()
        assertThat(resources.jobmanagerPVC).isNull()
        assertThat(resources.taskmanagerPVC).isNull()
    }

    @Test
    fun `should return resources when resources are changed`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val bootstrapJob = TestFactory.aBootstrapJob(cluster)
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
        val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
        val resources = cache.getCachedResources(clusterId = clusterId)
        assertThat(resources.bootstrapJob).isEqualTo(bootstrapJob)
        assertThat(resources.jobmanagerService).isEqualTo(jobManagerService1)
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobManagerStatefulSet1)
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskManagerStatefulSet1)
        assertThat(resources.jobmanagerPVC).isEqualTo(jobManagerPersistenVolumeClaim1)
        assertThat(resources.taskmanagerPVC).isEqualTo(taskManagerPersistenVolumeClaim1)
    }

    @Test
    fun `should update resources when resources are changed`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val bootstrapJob1 = TestFactory.aBootstrapJob(cluster1)
        val jobManagerService1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerStatefulSet1 = TestFactory.aJobManagerStatefulSet(cluster1)
        val taskManagerStatefulSet1 = TestFactory.aTaskManagerStatefulSet(cluster1)
        val jobManagerPersistenVolumeClaim1 = TestFactory.aJobManagerPersistenVolumeClaim(cluster1)
        val taskManagerPersistenVolumeClaim1 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val bootstrapJob2 = TestFactory.aBootstrapJob(cluster2)
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
        val clusterId1 = ClusterId(namespace = "flink", name = "test", uuid = "123")
        val resources1 = cache.getCachedResources(clusterId = clusterId1)
        assertThat(resources1.bootstrapJob).isEqualTo(bootstrapJob1)
        assertThat(resources1.jobmanagerService).isEqualTo(jobManagerService1)
        assertThat(resources1.jobmanagerStatefulSet).isEqualTo(jobManagerStatefulSet1)
        assertThat(resources1.taskmanagerStatefulSet).isEqualTo(taskManagerStatefulSet1)
        assertThat(resources1.jobmanagerPVC).isEqualTo(jobManagerPersistenVolumeClaim1)
        assertThat(resources1.taskmanagerPVC).isEqualTo(taskManagerPersistenVolumeClaim1)
        val clusterId2 = ClusterId(namespace = "flink", name = "test", uuid = "456")
        val resources2 = cache.getCachedResources(clusterId = clusterId2)
        assertThat(resources2.bootstrapJob).isEqualTo(bootstrapJob2)
        assertThat(resources2.jobmanagerService).isEqualTo(jobManagerService2)
        assertThat(resources2.jobmanagerStatefulSet).isEqualTo(jobManagerStatefulSet2)
        assertThat(resources2.taskmanagerStatefulSet).isEqualTo(taskManagerStatefulSet2)
        assertThat(resources2.jobmanagerPVC).isEqualTo(jobManagerPersistenVolumeClaim2)
        assertThat(resources2.taskmanagerPVC).isEqualTo(taskManagerPersistenVolumeClaim2)
    }

    @Test
    fun `should update resources when resources are deleted`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val bootstrapJob1 = TestFactory.aBootstrapJob(cluster1)
        val jobManagerService1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerStatefulSet1 = TestFactory.aJobManagerStatefulSet(cluster1)
        val taskManagerStatefulSet1 = TestFactory.aTaskManagerStatefulSet(cluster1)
        val jobManagerPersistenVolumeClaim1 = TestFactory.aJobManagerPersistenVolumeClaim(cluster1)
        val taskManagerPersistenVolumeClaim1 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val bootstrapJob2 = TestFactory.aBootstrapJob(cluster2)
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
        val clusterId1 = ClusterId(namespace = "flink", name = "test", uuid = "123")
        val resources1 = cache.getCachedResources(clusterId = clusterId1)
        assertThat(resources1.bootstrapJob).isNull()
        assertThat(resources1.jobmanagerService).isNull()
        assertThat(resources1.jobmanagerStatefulSet).isNull()
        assertThat(resources1.taskmanagerStatefulSet).isNull()
        assertThat(resources1.jobmanagerPVC).isNull()
        assertThat(resources1.taskmanagerPVC).isNull()
        val clusterId2 = ClusterId(namespace = "flink", name = "test", uuid = "456")
        val resources2 = cache.getCachedResources(clusterId = clusterId2)
        assertThat(resources2.bootstrapJob).isEqualTo(bootstrapJob2)
        assertThat(resources2.jobmanagerService).isEqualTo(jobManagerService2)
        assertThat(resources2.jobmanagerStatefulSet).isEqualTo(jobManagerStatefulSet2)
        assertThat(resources2.taskmanagerStatefulSet).isEqualTo(taskManagerStatefulSet2)
        assertThat(resources2.jobmanagerPVC).isEqualTo(jobManagerPersistenVolumeClaim2)
        assertThat(resources2.taskmanagerPVC).isEqualTo(taskManagerPersistenVolumeClaim2)
    }

    @Test
    fun `should update resources when all resources are deleted`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val bootstrapJob1 = TestFactory.aBootstrapJob(cluster1)
        val jobManagerService1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerStatefulSet1 = TestFactory.aJobManagerStatefulSet(cluster1)
        val taskManagerStatefulSet1 = TestFactory.aTaskManagerStatefulSet(cluster1)
        val jobManagerPersistenVolumeClaim1 = TestFactory.aJobManagerPersistenVolumeClaim(cluster1)
        val taskManagerPersistenVolumeClaim1 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster1)
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val bootstrapJob2 = TestFactory.aBootstrapJob(cluster2)
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
        cache.onJobDeletedAll()
        cache.onServiceDeletedAll()
        cache.onStatefulSetDeletedAll()
        cache.onStatefulSetDeletedAll()
        cache.onPersistentVolumeClaimDeletedAll()
        cache.onPersistentVolumeClaimDeletedAll()
        val clusterId1 = ClusterId(namespace = "flink", name = "test", uuid = "123")
        val resources1 = cache.getCachedResources(clusterId = clusterId1)
        assertThat(resources1.bootstrapJob).isNull()
        assertThat(resources1.jobmanagerService).isNull()
        assertThat(resources1.jobmanagerStatefulSet).isNull()
        assertThat(resources1.taskmanagerStatefulSet).isNull()
        assertThat(resources1.jobmanagerPVC).isNull()
        assertThat(resources1.taskmanagerPVC).isNull()
        val clusterId2 = ClusterId(namespace = "flink", name = "test", uuid = "456")
        val resources2 = cache.getCachedResources(clusterId = clusterId2)
        assertThat(resources2.bootstrapJob).isNull()
        assertThat(resources2.jobmanagerService).isNull()
        assertThat(resources2.jobmanagerStatefulSet).isNull()
        assertThat(resources2.taskmanagerStatefulSet).isNull()
        assertThat(resources2.jobmanagerPVC).isNull()
        assertThat(resources2.taskmanagerPVC).isNull()
    }
}
