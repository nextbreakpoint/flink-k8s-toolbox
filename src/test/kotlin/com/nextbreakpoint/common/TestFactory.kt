package com.nextbreakpoint.common

import com.nextbreakpoint.model.V1FlinkCluster
import com.nextbreakpoint.model.V1FlinkClusterSpec
import io.kubernetes.client.models.V1ObjectMeta

object TestFactory {
    fun aCluster(): V1FlinkCluster {
        val flinkClusterSpec = FlinkClusterSpecification.parse(
            """
            {
              "flinkImage": {
                "pullSecrets": "regcred",
                "pullPolicy": "IfNotPresent",
                "flinkImage": "registry:30000/flink:1.7.2"
              },
              "flinkJob": {
                "image": "registry:30000/flink-jobs:1",
                "jarPath": "/flink-jobs.jar",
                "className": "com.nextbreakpoint.flink.jobs.TestJob",
                "parallelism": 1,
                "arguments": [
                  "--BUCKET_BASE_PATH",
                  "file:///var/tmp"
                ]
              },
              "jobManager": {
                "serviceMode": "ClusterIP",
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
                "persistentVolumeClaimsTemplates": [
                  {
                    "metadata": {
                      "name": "jobmanager"
                    },
                    "spec": {
                      "storageClassName": "hostpath",
                      "accessModes": [ "ReadWriteOnce" ],
                      "resources": {
                        "requests": {
                          "storage": "1Gi"
                        }
                      }
                    }
                  }
                ]
              },
              "taskManager": {
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
                "persistentVolumeClaimsTemplates": [
                  {
                    "metadata": {
                      "name": "taskmanager"
                    },
                    "spec": {
                      "storageClassName": "hostpath",
                      "accessModes": [ "ReadWriteOnce" ],
                      "resources": {
                        "requests": {
                          "storage": "5Gi"
                        }
                      }
                    }
                  }
                ]
              },
              "flinkOperator": {
                "targetPath": "file:///var/tmp/test"
              }
            }
            """.trimIndent())
        return makeV1FlinkCluster("test", "flink", flinkClusterSpec)
    }

    private fun makeV1FlinkCluster(name: String, namespace: String, flinkClusterSpec: V1FlinkClusterSpec): V1FlinkCluster {
        val flinkCluster = V1FlinkCluster()
        val objectMeta = V1ObjectMeta().namespace(namespace).name(name)
        flinkCluster.metadata = objectMeta
        flinkCluster.spec = flinkClusterSpec
        return flinkCluster
    }
}