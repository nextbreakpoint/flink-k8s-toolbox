package com.nextbreakpoint.flink.core

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.io.{FileInputStream, InputStream, OutputStream}
import java.math.RoundingMode
import java.nio.charset.StandardCharsets
import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Date
import scala.io.Codec
import scala.util.{Failure, Success, Try}

case class SensorTemperature(eventId: String, eventTimestamp: Long, locationId: String, temperature: java.math.BigDecimal) {
  override def toString: String = {
    val builder = new StringBuilder()
    builder.append(Utility.format(Utility.convert(eventTimestamp)))
    builder.append(", ")
    builder.append(eventId)
    builder.append(", ")
    builder.append(locationId)
    builder.append(", ")
    builder.append(temperature)
    builder.toString()
  }
}

class AverageAccumulator(val temperature: java.math.BigDecimal, val count: Int) {
  def add(temperature: java.math.BigDecimal): AverageAccumulator =
    new AverageAccumulator(this.temperature.add(temperature), this.count + 1)

  def add(other: AverageAccumulator): AverageAccumulator =
    new AverageAccumulator(this.temperature.add(other.temperature), this.count + other.count)
}

class MaximumAccumulator(val temperature: java.math.BigDecimal) {
  def add(temperature: java.math.BigDecimal): MaximumAccumulator =
    new MaximumAccumulator(this.temperature.max(temperature))

  def add(other: MaximumAccumulator): MaximumAccumulator =
    new MaximumAccumulator(this.temperature.max(other.temperature))
}

class AverageAggregate() extends AggregateFunction[SensorTemperature, AverageAccumulator, java.math.BigDecimal] {
  override def createAccumulator(): AverageAccumulator =
    new AverageAccumulator(java.math.BigDecimal.ZERO, 0)

  override def add(value: SensorTemperature, accumulator: AverageAccumulator): AverageAccumulator = accumulator.add(value.temperature)

  override def merge(a: AverageAccumulator, b: AverageAccumulator): AverageAccumulator = a.add(b)

  override def getResult(accumulator: AverageAccumulator): java.math.BigDecimal =
    accumulator.temperature.divide(java.math.BigDecimal.valueOf(accumulator.count), 2, RoundingMode.HALF_DOWN)
}

class MaximumAggregate() extends AggregateFunction[SensorTemperature, MaximumAccumulator, java.math.BigDecimal] {
  override def createAccumulator(): MaximumAccumulator =
    new MaximumAccumulator(new java.math.BigDecimal(Double.MinValue))

  override def add(value: SensorTemperature, accumulator: MaximumAccumulator): MaximumAccumulator = accumulator.add(value.temperature)

  override def merge(a: MaximumAccumulator, b: MaximumAccumulator): MaximumAccumulator = a.add(b)

  override def getResult(accumulator: MaximumAccumulator): java.math.BigDecimal = accumulator.temperature
}

@SerialVersionUID(1L)
class SensorTemperatureBucketAssigner() extends BasePathBucketAssigner[SensorTemperature] {
  object EventBucketAssigner {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd--HH").withZone(ZoneId.of("UTC"))
  }

  override def getBucketId(element: SensorTemperature, context: BucketAssigner.Context): String =
    EventBucketAssigner.formatter.format(Utility.convert(element.eventTimestamp))
}

@SerialVersionUID(1L)
class SensorTemperatureEncoder() extends Object with Encoder[SensorTemperature] {
  override def encode(element: SensorTemperature, stream: OutputStream): Unit = {
    stream.write(Utility.format(Utility.convert(element.eventTimestamp)).getBytes(StandardCharsets.UTF_8))
    stream.write(',')
    stream.write(element.eventId.getBytes(StandardCharsets.UTF_8))
    stream.write(',')
    stream.write(element.locationId.getBytes(StandardCharsets.UTF_8))
    stream.write(',')
    stream.write(element.temperature.toString.getBytes(StandardCharsets.UTF_8))
    stream.write('\n')
  }
}

@SerialVersionUID(1L)
class SensorTemperatureTimestampAssigner() extends Object with SerializableTimestampAssigner[SensorTemperature] {
  override def extractTimestamp(element: SensorTemperature, recordTimestamp: Long): Long = element.eventTimestamp
}

case class LocationTemperature(locationId: String, eventTimestamp: Long, temperature: java.math.BigDecimal) {
  override def toString: String = {
    val builder = new StringBuilder()
    builder.append(Utility.format(Utility.convert(eventTimestamp)))
    builder.append(", ")
    builder.append(locationId)
    builder.append(", ")
    builder.append(temperature)
    builder.toString()
  }
}

@SerialVersionUID(1L)
class LocationTemperatureBucketAssigner() extends BasePathBucketAssigner[LocationTemperature] {
  object AverageBucketAssigner {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd--HH").withZone(ZoneId.of("UTC"))
  }

  override def getBucketId(element: LocationTemperature, context: BucketAssigner.Context) =
    AverageBucketAssigner.formatter.format(Utility.convert(element.eventTimestamp))
}

@SerialVersionUID(1L)
class LocationTemperatureEncoder() extends Object with Encoder[LocationTemperature] {
  override def encode(element: LocationTemperature, stream: OutputStream): Unit = {
    stream.write(Utility.format(Utility.convert(element.eventTimestamp)).getBytes(StandardCharsets.UTF_8))
    stream.write(',')
    stream.write(element.locationId.getBytes(StandardCharsets.UTF_8))
    stream.write(',')
    stream.write(element.temperature.toString.getBytes(StandardCharsets.UTF_8))
    stream.write('\n')
  }
}

class LocationTemperatureProcess() extends ProcessWindowFunction[java.math.BigDecimal, LocationTemperature, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[java.math.BigDecimal], out: Collector[LocationTemperature]): Unit = {
    elements.foreach { temperature =>
      out.collect(LocationTemperature(key, context.window.getStart, temperature))
    }
  }
}

object Utility {
  private val formatter = DateTimeFormatter.ofPattern("yyyy'-'MM'-'dd'T'HH:mm:ss.SSS'Z'")

  def parse(date: String): Long = LocalDateTime.from(formatter.parse(date)).toInstant(ZoneOffset.UTC).toEpochMilli

  def format(date: Date): String = formatter.format(LocalDateTime.ofInstant(date.toInstant, ZoneId.of("UTC")))

  def format(date: LocalDateTime): String = formatter.format(date)

  def convert(timestamp: Long): LocalDateTime = LocalDateTime.ofInstant(new Date(timestamp).toInstant, ZoneId.of("UTC"))

  def asInputStream(path: String): Try[InputStream] = asInputStream(path, getClass.getClassLoader)

  def asInputStream(path: String, classLoader: ClassLoader): Try[InputStream] =
    Try(
      path match {
        case p if p.startsWith("classpath:") => classLoader.getResourceAsStream(path.substring(10))
        case p if p.startsWith("file:") => new FileInputStream(path.substring(5))
        case _ => new FileInputStream(path)
      }
    )

  def readLines(inputStream: InputStream): Try[Iterator[String]] = Try(readAllLines(inputStream))

  private def readAllLines(inputStream: InputStream): Iterator[String] =
    scala.io.Source.fromInputStream(inputStream)(Codec.UTF8).getLines()

  def useResource[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    Try(block(resource)) match {
      case Success(result) =>
        resource.close()
        result
      case Failure(e) =>
        e.printStackTrace()
        resource.close()
        throw e
    }
  }

  def getParameters(inputStream: InputStream) = {
    Utility.useResource(inputStream) { stream =>
      val regex = "\\s*([a-zA-Z\\-]+)\\s*:\\s*(.+)\\s*".r
      Utility.readLines(stream).flatMap(
        lines => Try.apply(
          lines.map(
            line => regex.pattern.matcher(line)
          ).filter(
            matcher => matcher.matches()
          ).map(
            matcher => (matcher.group(1), matcher.group(2))
          ).toMap
        )
      )
    }
  }
}
