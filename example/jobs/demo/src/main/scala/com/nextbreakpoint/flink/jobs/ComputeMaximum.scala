package com.nextbreakpoint.flink.jobs

import com.nextbreakpoint.flink.core._
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.net.URI
import java.time.Duration
import java.util.Date

object ComputeMaximum {
  def main(args: Array[String]) {
    val params: ParameterTool = ParameterTool.fromArgs(args)

    val developerMode = System.getProperty("enable.developer.mode", "false").toBoolean

    val outputLocation = params.get("OUTPUT_LOCATION", "file:///tmp")
    val parameters = params.get("JOB_PARAMETERS", "/parameters.conf")

    val fileSystem = FileSystem.getUnguardedFileSystem(new URI(parameters))

    val maybeParameters = Utility.getParameters(fileSystem.open(new Path(parameters)))

    if (maybeParameters.isFailure) {
      throw new IllegalStateException("Can't parse job parameters")
    }

    val jobParameters = maybeParameters.get

    val date = Utility.format(new Date())

    val jobName = jobParameters.getOrElse("job-name", s"job-$date")
    val restPort = jobParameters.getOrElse("rest-port", "8081").toInt
    val maxParallelism = jobParameters.getOrElse("max-parallelism", "128").toInt
    val bucketOutputPath = jobParameters.getOrElse("bucket-output-path", "/output")
    val consoleVerbosity = jobParameters.getOrElse("console-verbosity", "0").toInt
    val sourceDelayArray = jobParameters.getOrElse("source-delay-array", "100 200").split(",|\\s").map(s => s.trim.toLong)
    val sourceDelayInterval = jobParameters.getOrElse("source-delay-interval", "1000").toInt
    val sourceLimit = jobParameters.getOrElse("source-limit", "0").toInt
    val checkpointInterval = jobParameters.getOrElse("checkpoint-interval", Time.seconds(300).toMilliseconds.toString).toLong
    val bucketCheckInterval = jobParameters.getOrElse("bucket-check-interval", Time.seconds(30).toMilliseconds.toString).toLong
    val bucketRolloverInterval = jobParameters.getOrElse("bucket-rollover-interval", Time.seconds(300).toMilliseconds.toString).toLong
    val bucketInactivityInterval = jobParameters.getOrElse("bucket-inactivity-interval", Time.seconds(300).toMilliseconds.toString).toLong
    val idleTimeout = jobParameters.getOrElse("idle-timeout", Time.seconds(30).toMilliseconds.toString).toLong
    val autoWatermarkInterval = jobParameters.getOrElse("auto-watermark-interval", Time.seconds(1).toMilliseconds.toString).toLong
    val maxOutOfOrderness = jobParameters.getOrElse("max-out-of-orderness", Time.minutes(1).toMilliseconds.toString).toLong
    val restartAttempts = jobParameters.getOrElse("restart-attempts", "3").toInt
    val delayBetweenAttempts = jobParameters.getOrElse("delay-between-attempts", Time.minutes(5).toMilliseconds.toString).toLong
    val disableChaining = jobParameters.getOrElse("disable-chaining", "false").toBoolean
    val windowSize = jobParameters.getOrElse("window-size", Time.minutes(60).toMilliseconds.toString).toLong
    val windowSlide = jobParameters.getOrElse("window-slide", Time.minutes(5).toMilliseconds.toString).toLong
    val partitions = jobParameters.getOrElse("partitions", "1").toInt

    val env = createExecutionEnvironment(developerMode, restPort, partitions)

    env.enableCheckpointing(checkpointInterval, CheckpointingMode.AT_LEAST_ONCE)

    env.getConfig.setAutoWatermarkInterval(autoWatermarkInterval)

    val restartStrategy = if (developerMode) {
      RestartStrategies.noRestart
    } else {
      RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts)
    }

    env.setRestartStrategy(restartStrategy)

    if (disableChaining) {
      env.disableOperatorChaining()
    }

    env.setMaxParallelism(maxParallelism)

    val parallelism = env.getParallelism

    StreamExecutionEnvironment.setDefaultLocalParallelism(parallelism)

    val source = new Source(
      delayArray = sourceDelayArray,
      delayInterval = sourceDelayInterval,
      shutdownDelay = idleTimeout * 2,
      partitions = Math.min(partitions, maxParallelism),
      parallelism = parallelism,
      limit = sourceLimit
    )

    val watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(maxOutOfOrderness))
      .withIdleness(Duration.ofMillis(idleTimeout))
      .withTimestampAssigner(new SensorTemperatureTimestampAssigner())

    val temperature = env.addSource(source)
      .uid("source")
      .name("source")
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid("sensor-temperature")
      .name("sensor-temperature")

    val maximum = temperature
      .keyBy(event => event.locationId)
      .window(SlidingEventTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds(windowSlide)))
      .aggregate(new MaximumAggregate(), new LocationTemperatureProcess())
      .uid("maximum-temperature")
      .name("maximum-temperature")

    val eventSink = StreamingFileSink.forRowFormat(new Path(outputLocation + bucketOutputPath + "/sensor-temperature"), new SensorTemperatureEncoder)
      .withBucketCheckInterval(bucketCheckInterval)
      .withBucketAssigner(new SensorTemperatureBucketAssigner())
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(bucketRolloverInterval)
          .withInactivityInterval(bucketInactivityInterval)
          .withMaxPartSize(1024 * 1024)
          .build()
      )
      .build()

    val maximumSink = StreamingFileSink.forRowFormat(new Path(outputLocation + bucketOutputPath + "/maximum-temperature"), new LocationTemperatureEncoder)
      .withBucketCheckInterval(bucketCheckInterval)
      .withBucketAssigner(new LocationTemperatureBucketAssigner())
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(bucketRolloverInterval)
          .withInactivityInterval(bucketInactivityInterval)
          .withMaxPartSize(1024 * 1024)
          .build()
      )
      .build()

    temperature
      .addSink(eventSink)
      .uid("output-sensor-temperature")
      .name("output-sensor-temperature")

    maximum
      .addSink(maximumSink)
      .uid("output-maximum-temperature")
      .name("output-maximum-temperature")

    if (consoleVerbosity > 1) {
      temperature
        .print("sensor")
        .setParallelism(1)
        .uid("print-sensor-temperature")
        .name("print-sensor-temperature")
    }

    if (consoleVerbosity > 0) {
      maximum
        .print("maximum")
        .setParallelism(1)
        .uid("print-maximum-temperature")
        .name("print-maximum-temperature")
    }

    val result = env.execute(jobName)

    System.out.println(result.toString)
  }

  private def createExecutionEnvironment(developerMode: Boolean, restPort: Int, parallelism: Int) = {
    if (developerMode) {
      val configuration = new Configuration
      configuration.setInteger("rest.port", restPort)
      configuration.setInteger("parallelism.default", parallelism)
      configuration.setString("metrics.reporters", "prometheus")
      configuration.setString("metrics.reporter.prometheus.class", "org.apache.flink.metrics.prometheus.PrometheusReporter")
      configuration.setString("metrics.reporter.prometheus.port", "9250")
      configuration.setString("state.backend", System.getProperty("state.backend", "filesystem"))
      configuration.setString("state.checkpoints.dir", System.getProperty("state.checkpoints.dir", "file:///tmp/checkpoints"))
      configuration.setString("state.savepoints.dir", System.getProperty("state.savepoints.dir", "file:///tmp/savepoints"))
      configuration.setString("s3.access-key", System.getProperty("s3.access-key", "minioaccesskey"))
      configuration.setString("s3.secret-key", System.getProperty("s3.secret-key", "miniosecretkey"))
      configuration.setString("s3.path.style.access", System.getProperty("s3.path.style.access", "true"))
      configuration.setString("s3.endpoint", System.getProperty("s3.endpoint", "http://localhost:9000"))
      FileSystem.initialize(configuration, null)
      StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    } else {
      StreamExecutionEnvironment.getExecutionEnvironment
    }
  }
}
