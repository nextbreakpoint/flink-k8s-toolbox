package com.nextbreakpoint.flink.k8s.supervisor

import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import com.nextbreakpoint.flink.k8s.supervisor.core.Adapter
import io.kubernetes.client.extended.controller.ControllerManager
import io.kubernetes.client.extended.controller.LeaderElectingController
import io.kubernetes.client.extended.controller.builder.ControllerBuilder
import io.kubernetes.client.extended.leaderelection.LeaderElectionConfig
import io.kubernetes.client.extended.leaderelection.LeaderElector
import io.kubernetes.client.extended.leaderelection.resourcelock.EndpointsLock
import io.kubernetes.client.informer.SharedInformerFactory
import io.micrometer.core.instrument.MeterRegistry
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.max
import kotlin.math.min

class SupervisorRunner(
    private val registry: MeterRegistry,
    private val controller: Controller,
    private val cache: Cache,
    private val factory: SharedInformerFactory,
    private val adapter: Adapter,
    private val options: RunnerOptions
) {
    companion object {
        private val logger = Logger.getLogger(SupervisorRunner::class.simpleName)
    }

    fun run() {
        val supervisorController = SupervisorController(registry, controller, cache, adapter, options)

        val controllerManager: ControllerManager = ControllerBuilder.controllerManagerBuilder(factory)
            .addController(supervisorController)
            .build()

        val leaderElectingController = LeaderElectingController(
            LeaderElector(
                LeaderElectionConfig(
                    EndpointsLock(cache.namespace, "leader-election-supervisor-${cache.clusterName}", "supervisor-${UUID.randomUUID()}"),
                    Duration.ofMillis(10000),
                    Duration.ofMillis(8000),
                    Duration.ofMillis(5000)
                )
            ),
            controllerManager
        )

        leaderElectingController.run()
    }

    private class SupervisorController(
        private val registry: MeterRegistry,
        private val controller: Controller,
        private val cache: Cache,
        private val adapter: Adapter,
        private val options: RunnerOptions
    ) : io.kubernetes.client.extended.controller.Controller {
        @Volatile
        private var running = true

        override fun run() {
            try {
                val supervisor = Supervisor.create(controller, cache, options.taskTimeout, options.pollingInterval, options.serverConfig)

                val pollingInterval = max(min(options.pollingInterval, 60L), 5L)

                while (running) {
                    try {
                        if (adapter.haveSynced()) {
                            cache.takeSnapshot()
                            supervisor.reconcile()
                            supervisor.cleanup()
                        } else {
                            logger.log(Level.INFO, "Cache not synced yet")
                        }

                        TimeUnit.SECONDS.sleep(pollingInterval)
                    } catch (e: Exception) {
                        logger.log(Level.SEVERE, "Something went wrong", e)
                    }
                }
            } catch (e: InterruptedException) {
                // do nothing
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Something went wrong", e)
            }
        }

        override fun shutdown() {
            running = false
        }
    }
}