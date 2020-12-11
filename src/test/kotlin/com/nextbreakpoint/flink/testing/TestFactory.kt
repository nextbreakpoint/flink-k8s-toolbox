package com.nextbreakpoint.flink.testing

import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
import io.kubernetes.client.openapi.models.V1JobBuilder
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder
import io.kubernetes.client.openapi.models.V1PodBuilder
import io.kubernetes.client.openapi.models.V1ServiceBuilder

object TestFactory {
    fun aFlinkCluster(name: String, namespace: String, taskManagers: Int = 1, taskSlots: Int = 1): V1FlinkCluster {
        val flinkClusterSpec = Resource.parseV1FlinkClusterSpec(
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
              }
            }
            """.trimIndent()
        )
        return makeFlinkCluster(
            name,
            namespace,
            flinkClusterSpec
        )
    }

    private fun makeFlinkCluster(name: String, namespace: String, flinkClusterSpec: V1FlinkClusterSpec): V1FlinkCluster {
        val objectMeta = V1ObjectMetaBuilder()
            .withNamespace(namespace)
            .withName(name)
            .build()
        return V1FlinkCluster.builder()
            .withApiVersion("nextbreakpoint.com/v1")
            .withKind("FlinkCluster")
            .withMetadata(objectMeta)
            .withSpec(flinkClusterSpec)
            .build()
    }

    fun aBootstrapJob(cluster: V1FlinkCluster, job: V1FlinkJob) = V1JobBuilder()
        .withNewMetadata()
        .withNamespace(job.metadata.namespace)
        .withName("bootstrap-${job.metadata.name}")
        .withLabels(mapOf(
            "jobName" to job.metadata.name,
            "clusterName" to cluster.metadata.name,
            "clusterUid" to cluster.metadata.uid
        ))
        .withUid(job.metadata.uid)
        .endMetadata()
        .build()

    fun aJobManagerService(cluster: V1FlinkCluster) = V1ServiceBuilder()
        .withNewMetadata()
        .withNamespace(cluster.metadata.namespace)
        .withName("jobmanager-${cluster.metadata.name}")
        .withLabels(mapOf(
            "clusterName" to cluster.metadata.name,
            "clusterUid" to cluster.metadata.uid
        ))
        .withUid(cluster.metadata.uid)
        .endMetadata()
        .build()

    fun aJobManagerPod(cluster: V1FlinkCluster, suffix: String) = V1PodBuilder()
        .withNewMetadata()
        .withNamespace(cluster.metadata.namespace)
        .withName("jobmanager-${cluster.metadata.name}-$suffix")
        .withLabels(mapOf(
            "clusterName" to cluster.metadata.name,
            "clusterUid" to cluster.metadata.uid,
            "role" to "jobmanager"
        ))
        .withUid(cluster.metadata.uid)
        .endMetadata()
        .build()

    fun aTaskManagerPod(cluster: V1FlinkCluster, suffix: String) = V1PodBuilder()
        .withNewMetadata()
        .withNamespace(cluster.metadata.namespace)
        .withName("taskmanager-${cluster.metadata.name}-$suffix")
        .withLabels(mapOf(
            "clusterName" to cluster.metadata.name,
            "clusterUid" to cluster.metadata.uid,
            "role" to "taskmanager"
        ))
        .withUid(cluster.metadata.uid)
        .endMetadata()
        .build()

    fun aFlinkJob(name: String, namespace: String): V1FlinkJob {
        val flinkJobSpec = Resource.parseV1FlinkJobSpec(
            """
            {
              "jobParallelism": 2,
              "bootstrap": {
                "serviceAccount": "bootstrap-test",
                "pullSecrets": "bootstrap-regcred",
                "pullPolicy": "IfNotPresent",
                "image": "registry:30000/jobs:latest",
                "jarPath": "/flink-jobs.jar",
                "className": "com.nextbreakpoint.flink.jobs.stream.TestJob",
                "arguments": [
                  "--BUCKET_BASE_PATH",
                  "file:///var/tmp"
                ]
              },
              "savepoint": {
                "savepointMode": "Automatic",
                "savepointInterval": "60",
                "savepointTargetPath": "file:///var/tmp/test"
              },
              "restart": {
                "restartPolicy": "Never",
                "restartDelay": 60,
                "restartTimeout": 120
              }
            }
            """.trimIndent()
        )
        return makeFlinkJob(
            name,
            namespace,
            flinkJobSpec
        )
    }

    private fun makeFlinkJob(name: String, namespace: String, flinkJobSpec: V1FlinkJobSpec): V1FlinkJob {
        val objectMeta = V1ObjectMetaBuilder()
            .withNamespace(namespace)
            .withName(name)
            .build()
        return V1FlinkJob.builder()
            .withApiVersion("nextbreakpoint.com/v1")
            .withKind("FlinkJob")
            .withMetadata(objectMeta)
            .withSpec(flinkJobSpec)
            .build()
    }
}
