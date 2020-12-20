package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.LaunchCommand
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.common.ServerConfig
import com.nextbreakpoint.flink.common.SupervisorOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.supervisor.SupervisorRunner
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import com.nextbreakpoint.flink.k8s.supervisor.core.Adapter
import com.nextbreakpoint.flink.vertex.MonitoringVerticle
import com.nextbreakpoint.flink.vertex.SupervisorVerticle
import io.vertx.core.VertxOptions
import io.vertx.micrometer.MicrometerMetricsOptions
import io.vertx.micrometer.VertxPrometheusOptions
import io.vertx.micrometer.backends.BackendRegistries
import io.vertx.rxjava.core.Vertx
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class LaunchSupervisor : LaunchCommand<SupervisorOptions> {
    companion object {
        private val kubeClient = KubeClient
        private val flinkClient = FlinkClient
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, options: SupervisorOptions) {
        val controlPort = Integer.getInteger("supervisor.control.port", 4444)
        val monitoringPort = Integer.getInteger("supervisor.monitoring.port", 8080)

        val serverConfig = ServerConfig(
            port = controlPort,
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

        val cache = Cache(namespace = namespace, clusterName = options.clusterName)

        val controller = Controller(flinkOptions, flinkClient, kubeClient, options.dryRun)

        val vertx = Vertx.vertx(createVertxOptions())

        vertx.deployVerticle(MonitoringVerticle(monitoringPort, "flink-supervisor"))
        vertx.deployVerticle(SupervisorVerticle(namespace, options.clusterName, cache, controller, serverConfig))

        val registry = BackendRegistries.getNow("flink-supervisor")

        val cacheAdapter = Adapter(kubeClient, cache)

        val runner = SupervisorRunner(registry, controller, cache, cacheAdapter, runnerOptions)

        val thread = thread {
            runner.run()
        }

        thread.join()
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
                .setRegistryName("flink-supervisor")

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
