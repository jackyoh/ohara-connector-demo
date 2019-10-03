package idv.jack.ohara

import java.util

import com.island.ohara.common.data.{Cell, Column, DataType, Row}
import com.island.ohara.common.util.{ByteUtils, CommonUtils}
import com.island.ohara.kafka.connector.{RowSourceRecord, RowSourceTask, TaskSetting}

import scala.collection.JavaConverters._

class RandomNumberTask extends RowSourceTask {
  private[perf] var schema: Seq[Column] = _
  private[this] var topics: Seq[String] = _

  override def _start(settings: TaskSetting): Unit = {
    this.topics = settings.topicNames().asScala
    this.schema = settings.columns.asScala
  }

  override def _stop(): Unit = {

  }

  override def _poll(): util.List[RowSourceRecord] = {
    Thread.sleep(5000)
    val value = CommonUtils.randomString()

    val row: Row = Row.of(
      schema.sortBy(_.order).map { c =>
        Cell.of(
          c.name,
          c.dataType match {
            case DataType.BOOLEAN => false
            case DataType.BYTE    => ByteUtils.toBytes(value).head
            case DataType.BYTES   => ByteUtils.toBytes(value)
            case DataType.SHORT   => value.toShort
            case DataType.INT     => value.toInt
            case DataType.LONG    => value
            case DataType.FLOAT   => value.toFloat
            case DataType.DOUBLE  => value.toDouble
            case DataType.STRING  => value.toString
            case _                => value
          }
        )
      }: _*
    )
    val records: Seq[RowSourceRecord] = topics.map(RowSourceRecord.builder().row(row).topicName(_).build())
    records.toList.asJava
  }
}
