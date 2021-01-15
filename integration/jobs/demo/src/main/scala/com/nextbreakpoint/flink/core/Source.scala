package com.nextbreakpoint.flink.core

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class Source(
   val delayArray: Array[Long],
   val delayInterval: Long,
   val shutdownDelay: Long,
   val partitions: Int,
   val parallelism: Int,
   val limit: Int
) extends RichParallelSourceFunction[SensorTemperature] with CheckpointedFunction {
  object Source {
    val logger = LoggerFactory.getLogger(classOf[Source].getName)
  }

  @volatile
  private var interrupted = false

  @transient
  private var partitionOffsets: mutable.Map[Int, Int] = null

  @transient
  private var partitionOffsetsState: ListState[org.apache.flink.api.java.tuple.Tuple2[Int, Int]] = null

  override def run(ctx: SourceFunction.SourceContext[SensorTemperature]): Unit = {
    try {
      emitEventsUntilInterrupted(ctx)

      ctx.markAsTemporarilyIdle()

      if (shutdownDelay > 0) {
        Thread.sleep(shutdownDelay)
      }
    } finally {
      ctx.close()
    }
  }

  override def cancel(): Unit = {
    interrupted = true
  }

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val taskIndex = getRuntimeContext.getIndexOfThisSubtask
    val taskCount = getRuntimeContext.getNumberOfParallelSubtasks

    initState(taskIndex, taskCount)
  }

  @throws[Exception]
  override def close(): Unit = {
    partitionOffsetsState.clear()
    partitionOffsets.clear()

    super.close()
  }

  @throws[Exception]
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    val taskIndex = getRuntimeContext.getIndexOfThisSubtask
    val taskCount = getRuntimeContext.getNumberOfParallelSubtasks

    initState(taskIndex, taskCount)

    partitionOffsetsState.clear()
    partitionOffsetsState.addAll(
      partitionOffsets.map(t => new org.apache.flink.api.java.tuple.Tuple2(t._1, t._2)).toList.asJava
    )
  }

  @throws[Exception]
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val taskName = getRuntimeContext.getTaskName
    val taskIndex = getRuntimeContext.getIndexOfThisSubtask
    val taskCount = getRuntimeContext.getNumberOfParallelSubtasks

    initState(taskIndex, taskCount)

    val typeInfo = TypeInformation.of(new TypeHint[tuple.Tuple2[Int, Int]]() {})
    val descriptor = new ListStateDescriptor("offsets", typeInfo)
    partitionOffsetsState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      Source.logger.info("Restoring state from snapshot...")

      for (entry <- partitionOffsetsState.get().asScala) {
        partitionOffsets.update(entry.f0, entry.f1)
      }
    }

    printState("initializeState", taskName, taskIndex, taskCount)
  }

  private def emitEventsUntilInterrupted(ctx: SourceFunction.SourceContext[SensorTemperature]) = {
      var lastTime = System.currentTimeMillis()

      var index = 0

      val maxIndex = (limit + parallelism / 2) / parallelism

      do {
        val time = System.currentTimeMillis()

        val events = mutable.MutableList[(SensorTemperature, Long)]()

        ctx.getCheckpointLock.synchronized {
          for (entry <- partitionOffsets) {
            val partition = entry._1
            val offset = entry._2

            if (time - lastTime > delayInterval) {
              partitionOffsets.update(partition, offset + 1)
            }

            events += Tuple2(createEvent(time, partition), delayArray(offset % delayArray.length))

            index += 1
          }
        }

        if (time - lastTime > delayInterval) {
          lastTime = time
        }

        for (event <- events) {
          if (maxIndex <= 0 || index < maxIndex) {
            ctx.collect(event._1)
            Thread.sleep(event._2 * parallelism)
          }
        }

      } while (!interrupted && (maxIndex <= 0 || index < maxIndex))
  }

  private def createEvent(timestamp: Long, partition: Int) =
    SensorTemperature(
      eventId = UUID.randomUUID().toString,
      eventTimestamp = timestamp,
      locationId = new UUID(0, partition).toString,
      temperature = java.math.BigDecimal.valueOf(Random.nextDouble() * 10 + 20)
    )

  private def initState(taskIndex: Int, taskCount: Int) = {
    if (partitionOffsets == null) {
      partitionOffsets = mutable.Map[Int, Int]()
    }

    if (partitionOffsets.isEmpty) {
      partitionOffsets ++= (0 until partitions / taskCount).map(index => (taskIndex * (partitions / taskCount) + index, 0))
    }
  }

  private def printState(method: String, taskName: String, taskIndex: Int, taskCount: Int) = {
    for (partitionOffset <- partitionOffsets) {
      val partition = partitionOffset._1
      val offset = partitionOffset._2
      Source.logger.info(s"$taskName (${taskIndex + 1}/$taskCount) $method - partition: $partition, offset: $offset")
    }
  }
}
