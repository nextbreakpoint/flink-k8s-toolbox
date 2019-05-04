package com.nextbreakpoint.common

import com.nextbreakpoint.model.V1FlinkCluster
import com.nextbreakpoint.model.V1FlinkClusterSpec
import io.kubernetes.client.models.V1ObjectMeta

object TestFactory {
    fun aCluster(): V1FlinkCluster {
        val flinkClusterSpec = FlinkClusterSpecification.parse("""
        {
            "pullSecrets": "regcred",
            "pullPolicy": "IfNotPresent",
            "flinkImage": "registry:30000/flink:1.7.2",
            "jobImage": "registry:30000/flink-jobs:1",
            "jobJarPath": "/flink-jobs-1.0.0.jar",
            "jobClassName": "com.nextbreakpoint.flink.jobs.TestJob",
            "jobParallelism": 1,
            "jobArguments": [
              "--BUCKET_BASE_PATH",
              "file:///var/tmp"
            ],
            "jobmanagerStorageClass": "hostpath",
            "jobmanagerEnvironment": [
              {
                "name": "FLINK_GRAPHITE_HOST",
                "value": "graphite.default.svc.cluster.local"
              }
            ],
            "taskmanagerStorageClass": "hostpath",
            "taskmanagerEnvironment": [
              {
                "name": "FLINK_GRAPHITE_HOST",
                "value": "graphite.default.svc.cluster.local"
              }
            ]
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