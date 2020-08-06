package com.nextbreakpoint.flink.testing

import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterJobSpec
import io.kubernetes.client.openapi.models.V1JobBuilder
import io.kubernetes.client.openapi.models.V1ObjectMeta
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder
import io.kubernetes.client.openapi.models.V1PodBuilder
import io.kubernetes.client.openapi.models.V1ServiceBuilder

object TestFactory {
    fun aFlinkCluster(name: String, namespace: String, taskManagers: Int = 1, taskSlots: Int = 1): V2FlinkCluster {
        val flinkClusterSpec = Resource.parseV2FlinkClusterSpec(
            """
            {
              "taskManagers": $taskManagers,
              "runtime": {
                "pullSecrets": "flink-regcred",
                "pullPolicy": "IfNotPresent",
                "image": "registry:30000/flink:1.9.2"
              },
              "jobManager": {
                "serviceAccount": "jobmanager-test",
                "serviceMode": "ClusterIP",
                "maxHeapMemory": 512,
                "environment": [
                  {
                    "name": "FLINK_GRAPHITE_HOST",
                    "value": "graphite.default.svc.cluster.local"
                  }
                ],
                "environmentFrom": [
                  {
                    "secretRef": {
                      "name": "flink-secrets"
                    }
                  }
                ],
                "volumeMounts": [
                  {
                    "name": "config-vol",
                    "mountPath": "/hadoop/etc/core-site.xml",
                    "subPath": "core-site.xml"
                  },
                  {
                    "name": "config-vol",
                    "mountPath": "/docker-entrypoint.sh",
                    "subPath": "docker-entrypoint.sh"
                  },
                  {
                    "name": "config-vol",
                    "mountPath": "/opt/flink/conf/flink-conf.yaml",
                    "subPath": "flink-conf.yaml"
                  },
                  {
                    "name": "jobmanager",
                    "mountPath": "/var/tmp"
                  }
                ],
                "volumes": [
                  {
                    "name": "config-vol",
                    "configMap": {
                      "name": "flink-config",
                      "defaultMode": "511"
                    }
                  }
                ],
                "initContainers": [
                    {
                        "image": "busybox",
                        "command": [
                            "ls"
                        ],
                        "imagePullPolicy": "IfNotPresent",
                        "name": "busybox"
                    }
                ],
                "sideContainers": [
                    {
                        "image": "busybox",
                        "command": [
                            "sleep",
                            "3600"
                        ],
                        "imagePullPolicy": "IfNotPresent",
                        "name": "busybox"
                    }
                ],
                "resources": {
                    "limits": {
                        "cpu": "1",
                        "memory": "512Mi"
                    },
                    "requests": {
                        "cpu": "0.1",
                        "memory": "256Mi"
                    }
                }
              },
              "taskManager": {
                "serviceAccount": "taskmanager-test",
                "taskSlots": $taskSlots,
                "maxHeapMemory": 2048,
                "environment": [
                  {
                    "name": "FLINK_GRAPHITE_HOST",
                    "value": "graphite.default.svc.cluster.local"
                  }
                ],
                "volumeMounts": [
                  {
                    "name": "config-vol",
                    "mountPath": "/hadoop/etc/core-site.xml",
                    "subPath": "core-site.xml"
                  },
                  {
                    "name": "config-vol",
                    "mountPath": "/docker-entrypoint.sh",
                    "subPath": "docker-entrypoint.sh"
                  },
                  {
                    "name": "config-vol",
                    "mountPath": "/opt/flink/conf/flink-conf.yaml",
                    "subPath": "flink-conf.yaml"
                  },
                  {
                    "name": "taskmanager",
                    "mountPath": "/var/tmp"
                  }
                ],
                "volumes": [
                  {
                    "name": "config-vol",
                    "configMap": {
                      "name": "flink-config",
                      "defaultMode": "511"
                    }
                  }
                ],
                "initContainers": [
                    {
                        "image": "busybox",
                        "command": [
                            "ls"
                        ],
                        "imagePullPolicy": "IfNotPresent",
                        "name": "busybox"
                    }
                ],
                "sideContainers": [
                    {
                        "image": "busybox",
                        "command": [
                            "sleep",
                            "3600"
                        ],
                        "imagePullPolicy": "IfNotPresent",
                        "name": "busybox"
                    }
                ],
                "resources": {
                    "limits": {
                        "cpu": "1",
                        "memory": "2048Mi"
                    },
                    "requests": {
                        "cpu": "0.2",
                        "memory": "1024Mi"
                    }
                }
              },
              "supervisor": {
              },
              "jobs": [{
                  "name": "test",
                  "spec": {
                      "jobParallelism": 2,
                      "bootstrap": {
                        "serviceAccount": "bootstrap-test",
                        "pullSecrets": "bootstrap-regcred",
                        "pullPolicy": "IfNotPresent",
                        "image": "registry:30000/flink-jobs:1",
                        "jarPath": "/flink-jobs.jar",
                        "className": "com.nextbreakpoint.flink.jobs.stream.TestJob",
                        "arguments": [
                          "--BUCKET_BASE_PATH",
                          "file:///var/tmp"
                        ]
                      },
                      "savepoint": {
                        "restartPolicy": "Never",
                        "savepointMode": "Automatic",
                        "savepointInterval": "60",
                        "savepointTargetPath": "file:///var/tmp/test"
                      }
                  }
              }]
            }
            """.trimIndent()
        )
        return makeV2FlinkCluster(
            name,
            namespace,
            flinkClusterSpec
        )
    }

    private fun makeV2FlinkCluster(name: String, namespace: String, flinkClusterSpec: V2FlinkClusterSpec): V2FlinkCluster {
        val flinkCluster = V2FlinkCluster()
            .apiVersion("nextbreakpoint.com/v2")
            .kind("FlinkCluster")
        val objectMeta = V1ObjectMeta().namespace(namespace).name(name)
        flinkCluster.metadata = objectMeta
        flinkCluster.spec = flinkClusterSpec
        return flinkCluster
    }

    fun aBootstrapJob(cluster: V2FlinkCluster) = V1JobBuilder()
        .withNewMetadata()
        .withNamespace(cluster.metadata.namespace)
        .withName("${cluster.metadata.name}-job")
        .withLabels(mapOf(
            "jobName" to cluster.spec.jobs[0].name,
            "clusterName" to cluster.metadata.name,
            "clusterUid" to cluster.metadata.uid
        ))
        .withUid(cluster.metadata.uid)
        .endMetadata()
        .build()

    fun aJobManagerService(cluster: V2FlinkCluster) = V1ServiceBuilder()
        .withNewMetadata()
        .withNamespace(cluster.metadata.namespace)
        .withName("${cluster.metadata.name}-service")
        .withLabels(mapOf(
            "clusterName" to cluster.metadata.name,
            "clusterUid" to cluster.metadata.uid
        ))
        .withUid(cluster.metadata.uid)
        .endMetadata()
        .build()

    fun aJobManagerPod(cluster: V2FlinkCluster, suffix: String) = V1PodBuilder()
        .withNewMetadata()
        .withNamespace(cluster.metadata.namespace)
        .withName("${cluster.metadata.name}-pod-$suffix")
        .withLabels(mapOf(
            "clusterName" to cluster.metadata.name,
            "clusterUid" to cluster.metadata.uid,
            "role" to "jobmanager"
        ))
        .withUid(cluster.metadata.uid)
        .endMetadata()
        .build()

    fun aTaskManagerPod(cluster: V2FlinkCluster, suffix: String) = V1PodBuilder()
        .withNewMetadata()
        .withNamespace(cluster.metadata.namespace)
        .withName("${cluster.metadata.name}-pod-$suffix")
        .withLabels(mapOf(
            "clusterName" to cluster.metadata.name,
            "clusterUid" to cluster.metadata.uid,
            "role" to "taskmanager"
        ))
        .withUid(cluster.metadata.uid)
        .endMetadata()
        .build()

    fun aFlinkJob(cluster: V2FlinkCluster): V1FlinkJob {
        val jobSpec = cluster.spec.jobs.get(0)
        val metadata = V1ObjectMetaBuilder()
            .withLabels(mapOf(
                "owner" to "test",
                "jobName" to cluster.spec.jobs[0].name,
                "clusterName" to cluster.metadata.name,
                "clusterUid" to cluster.metadata.uid,
                "component" to "flink"
            ))
            .withNamespace(cluster.metadata.namespace)
            .withName("${cluster.metadata?.name}-${jobSpec.name}")
            .build()

        val resource = V1FlinkJob()
        resource.kind = "V1FlinkJob"
        resource.apiVersion = "v1"
        resource.metadata = metadata
        resource.spec = jobSpec.spec
        return resource
    }
}
