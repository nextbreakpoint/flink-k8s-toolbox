package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class SupervisorCacheTest {
    private val cache = SupervisorCache(namespace = "flink", clusterName = "test")

    @Test
    fun `cache is empty`() {
        assertThat(cache.getFlinkClusters()).isEmpty()
    }

    @Test
    fun `should throw RuntimeException when looking for non existent cluster`() {
        assertThatThrownBy { cache.getFlinkCluster(ClusterSelector(namespace = "flink", name = "test", uuid = "123")) }.isInstanceOf(RuntimeException::class.java)
    }

    @Test
    fun `should return null when looking for non existent cluster id`() {
        assertThat(cache.findClusterSelector(namespace = "flink", name = "test")).isNull()
    }

    @Test
    fun `should return empty list of clusters initially`() {
        assertThat(cache.getFlinkClusters()).isEmpty()
    }

    @Test
    fun `should return empty list of cluster selectors initially`() {
        assertThat(cache.getClusterSelectors()).isEmpty()
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
        assertThat(cache.getFlinkCluster(ClusterSelector(namespace = cluster.metadata.namespace, name = cluster.metadata.name, uuid = "123"))).isNotNull()
    }

    @Test
    fun `should find cluster selectors when a cluster changed`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        assertThat(cache.findClusterSelector(namespace = cluster.metadata.namespace, name = cluster.metadata.name)).isNotNull()
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
        val clusterSelectors = cache.getClusterSelectors()
        assertThat(clusterSelectors).hasSize(2)
        val clusterSelector1 = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
        val clusterSelector2 = ClusterSelector(namespace = "flink", name = "test", uuid = "456")
        assertThat(clusterSelectors).containsExactlyInAnyOrder(clusterSelector1, clusterSelector2)
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
        val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
        val resources = cache.getCachedResources(clusterSelector = clusterSelector)
        assertThat(resources.bootstrapJob).isNull()
        assertThat(resources.service).isNull()
        assertThat(resources.jobmanagerPods).isEmpty()
        assertThat(resources.taskmanagerPods).isEmpty()
    }

    @Test
    fun `should return resources when resources are changed`() {
        val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val bootstrapJob = TestFactory.aBootstrapJob(cluster)
        val service = TestFactory.aJobManagerService(cluster)
        val jobManagerPod = TestFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod = TestFactory.aTaskManagerPod(cluster,"1")
        cache.onJobChanged(bootstrapJob)
        cache.onServiceChanged(service)
        cache.onPodChanged(jobManagerPod)
        cache.onPodChanged(taskManagerPod)
        val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
        val resources = cache.getCachedResources(clusterSelector = clusterSelector)
        assertThat(resources.bootstrapJob).isEqualTo(bootstrapJob)
        assertThat(resources.service).isEqualTo(service)
        assertThat(resources.jobmanagerPods).isEqualTo(setOf(jobManagerPod))
        assertThat(resources.taskmanagerPods).isEqualTo(setOf(taskManagerPod))
    }

    @Test
    fun `should update resources when resources are changed`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val bootstrapJob1 = TestFactory.aBootstrapJob(cluster1)
        val service1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerPod1 = TestFactory.aJobManagerPod(cluster1,"1")
        val taskManagerPod1 = TestFactory.aTaskManagerPod(cluster1,"1")
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val bootstrapJob2 = TestFactory.aBootstrapJob(cluster2)
        val service2 = TestFactory.aJobManagerService(cluster2)
        val jobManagerPod2 = TestFactory.aJobManagerPod(cluster2,"1")
        val taskManagerPod2 = TestFactory.aTaskManagerPod(cluster2,"1")
        cache.onJobChanged(bootstrapJob1)
        cache.onServiceChanged(service1)
        cache.onPodChanged(jobManagerPod1)
        cache.onPodChanged(taskManagerPod1)
        cache.onJobChanged(bootstrapJob2)
        cache.onServiceChanged(service2)
        cache.onPodChanged(jobManagerPod2)
        cache.onPodChanged(taskManagerPod2)
        val clusterSelector1 = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
        val resources1 = cache.getCachedResources(clusterSelector = clusterSelector1)
        assertThat(resources1.bootstrapJob).isEqualTo(bootstrapJob1)
        assertThat(resources1.service).isEqualTo(service1)
        assertThat(resources1.jobmanagerPods).isEqualTo(setOf(jobManagerPod1))
        assertThat(resources1.taskmanagerPods).isEqualTo(setOf(taskManagerPod1))
        val clusterSelector2 = ClusterSelector(namespace = "flink", name = "test", uuid = "456")
        val resources2 = cache.getCachedResources(clusterSelector = clusterSelector2)
        assertThat(resources2.bootstrapJob).isEqualTo(bootstrapJob2)
        assertThat(resources2.service).isEqualTo(service2)
        assertThat(resources2.jobmanagerPods).isEqualTo(setOf(jobManagerPod2))
        assertThat(resources2.taskmanagerPods).isEqualTo(setOf(taskManagerPod2))
    }

    @Test
    fun `should update resources when resources are deleted`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val bootstrapJob1 = TestFactory.aBootstrapJob(cluster1)
        val service1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerPod1 = TestFactory.aJobManagerPod(cluster1,"1")
        val taskManagerPod1 = TestFactory.aTaskManagerPod(cluster1,"1")
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val bootstrapJob2 = TestFactory.aBootstrapJob(cluster2)
        val service2 = TestFactory.aJobManagerService(cluster2)
        val jobManagerPod2 = TestFactory.aJobManagerPod(cluster2,"1")
        val taskManagerPod2 = TestFactory.aTaskManagerPod(cluster2,"1")
        cache.onJobChanged(bootstrapJob1)
        cache.onServiceChanged(service1)
        cache.onPodChanged(jobManagerPod1)
        cache.onPodChanged(taskManagerPod1)
        cache.onJobChanged(bootstrapJob2)
        cache.onServiceChanged(service2)
        cache.onPodChanged(jobManagerPod2)
        cache.onPodChanged(taskManagerPod2)
        cache.onJobDeleted(bootstrapJob1)
        cache.onServiceDeleted(service1)
        cache.onPodDeleted(jobManagerPod1)
        cache.onPodDeleted(taskManagerPod1)
        val clusterSelector1 = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
        val resources1 = cache.getCachedResources(clusterSelector = clusterSelector1)
        assertThat(resources1.bootstrapJob).isNull()
        assertThat(resources1.service).isNull()
        assertThat(resources1.jobmanagerPods).isEmpty()
        assertThat(resources1.taskmanagerPods).isEmpty()
        val clusterSelector2 = ClusterSelector(namespace = "flink", name = "test", uuid = "456")
        val resources2 = cache.getCachedResources(clusterSelector = clusterSelector2)
        assertThat(resources2.bootstrapJob).isEqualTo(bootstrapJob2)
        assertThat(resources2.service).isEqualTo(service2)
        assertThat(resources2.jobmanagerPods).isEqualTo(setOf(jobManagerPod2))
        assertThat(resources2.taskmanagerPods).isEqualTo(setOf(taskManagerPod2))
    }

    @Test
    fun `should update resources when all resources are deleted`() {
        val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val bootstrapJob1 = TestFactory.aBootstrapJob(cluster1)
        val service1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerPod1 = TestFactory.aJobManagerPod(cluster1,"1")
        val taskManagerPod1 = TestFactory.aTaskManagerPod(cluster1,"1")
        val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val bootstrapJob2 = TestFactory.aBootstrapJob(cluster2)
        val service2 = TestFactory.aJobManagerService(cluster2)
        val jobManagerPod2 = TestFactory.aJobManagerPod(cluster2,"1")
        val taskManagerPod2 = TestFactory.aTaskManagerPod(cluster2,"1")
        cache.onJobChanged(bootstrapJob1)
        cache.onServiceChanged(service1)
        cache.onPodChanged(jobManagerPod1)
        cache.onPodChanged(taskManagerPod1)
        cache.onJobChanged(bootstrapJob2)
        cache.onServiceChanged(service2)
        cache.onPodChanged(jobManagerPod2)
        cache.onPodChanged(taskManagerPod2)
        cache.onJobDeletedAll()
        cache.onServiceDeletedAll()
        cache.onPodDeletedAll()
        cache.onPodDeletedAll()
        val clusterSelector1 = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
        val resources1 = cache.getCachedResources(clusterSelector = clusterSelector1)
        assertThat(resources1.bootstrapJob).isNull()
        assertThat(resources1.service).isNull()
        assertThat(resources1.jobmanagerPods).isEmpty()
        assertThat(resources1.taskmanagerPods).isEmpty()
        val clusterSelector2 = ClusterSelector(namespace = "flink", name = "test", uuid = "456")
        val resources2 = cache.getCachedResources(clusterSelector = clusterSelector2)
        assertThat(resources2.bootstrapJob).isNull()
        assertThat(resources2.service).isNull()
        assertThat(resources2.jobmanagerPods).isEmpty()
        assertThat(resources2.taskmanagerPods).isEmpty()
    }
}
