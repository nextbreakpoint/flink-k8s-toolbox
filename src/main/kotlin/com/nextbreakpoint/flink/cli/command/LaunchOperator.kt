package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.LaunchCommand
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.OperatorOptions
import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.common.ServerConfig
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.operator.OperatorRunner
import com.nextbreakpoint.flink.k8s.operator.core.Cache
import com.nextbreakpoint.flink.k8s.operator.core.Adapter
import com.nextbreakpoint.flink.vertex.MonitoringVerticle
import com.nextbreakpoint.flink.vertex.OperatorVerticle
import io.vertx.core.VertxOptions
import io.vertx.micrometer.MicrometerMetricsOptions
import io.vertx.micrometer.VertxPrometheusOptions
import io.vertx.micrometer.backends.BackendRegistries
import io.vertx.rxjava.core.Vertx
import java.util.concurrent.TimeUnit

class LaunchOperator : LaunchCommand<OperatorOptions> {
    companion object {
        private val kubeClient = KubeClient
        private val flinkClient = FlinkClient
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, options: OperatorOptions) {
        val monitoringPort = Integer.getInteger("monitoring.port", 8080)

        val serverConfig = ServerConfig(
            port = options.port,
            keystorePath = options.keystorePath,
            keystoreSecret = options.keystoreSecret,
            truststorePath = options.truststorePath,
            truststoreSecret = options.truststoreSecret
        )

        val runnerOptions = RunnerOptions(
            pollingInterval = options.pollingInterval,
            taskTimeout = options.taskTimeout,
            serverConfig = serverConfig
        )

        val cache = Cache(namespace)

        val controller = Controller(flinkOptions, flinkClient, kubeClient, options.dryRun)

        val vertx = Vertx.vertx(createVertxOptions())

        vertx.deployVerticle(MonitoringVerticle(monitoringPort, "flink-operator"))
        vertx.deployVerticle(OperatorVerticle(namespace, cache, controller, serverConfig))

        val registry = BackendRegistries.getNow("flink-operator")

        val factory = kubeClient.createSharedInformerFactory()

        val cacheAdapter = Adapter(kubeClient, cache, factory)

        try {
            cacheAdapter.start()

            val runner = OperatorRunner(registry, controller, cache, factory, cacheAdapter, runnerOptions)

            runner.run()
        } finally {
            cacheAdapter.stop()
        }
    }

    private fun createVertxOptions(): VertxOptions {
        val vertxOptions = VertxOptions()
        vertxOptions.metricsOptions =
            MicrometerMetricsOptions().setPrometheusOptions(
                VertxPrometheusOptions()
                    .setEnabled(true)
//                    .setStartEmbeddedServer(true)
//                    .setEmbeddedServerEndpoint("/metrics")
//                    .setEmbeddedServerOptions(
//                        HttpServerOptions().setSsl(false).setPort(8080)
//                    )
            )
                .setEnabled(true)
                .setRegistryName("flink-operator")

        vertxOptions.workerPoolSize = 4
        vertxOptions.eventLoopPoolSize = 1
        vertxOptions.maxWorkerExecuteTime = 30
        vertxOptions.maxWorkerExecuteTimeUnit = TimeUnit.SECONDS
        vertxOptions.warningExceptionTime = 20
        vertxOptions.warningExceptionTimeUnit = TimeUnit.SECONDS
        vertxOptions.maxEventLoopExecuteTime = 30
        vertxOptions.maxEventLoopExecuteTimeUnit = TimeUnit.SECONDS
        return vertxOptions
    }
}
