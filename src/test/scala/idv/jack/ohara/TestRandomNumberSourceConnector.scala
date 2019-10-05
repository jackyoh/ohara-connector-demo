package idv.jack.ohara

import com.island.ohara.client.kafka.WorkerClient
import com.island.ohara.common.data.{Column, DataType, Row, Serializer}
import com.island.ohara.common.setting.{ConnectorKey, TopicKey}
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.kafka.Consumer
import com.island.ohara.testing.service.{Brokers, Workers, Zookeepers}
import org.junit.Test
import org.scalatest.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.Await


class TestRandomNumberSourceConnector extends Matchers{

  @Test
  def test(): Unit = {
    val brokerNumber = 1
    val workerNumber = 1
    val zookeeper = Zookeepers.local(0)
    try {
      val brokers = Brokers.local(zookeeper, (1 to brokerNumber).map(x => 0).toArray)
      try {
        val workers = Workers.local(brokers, (1 to workerNumber).map(x => 0).toArray)
        try {
          val workerClient = WorkerClient(workers.connectionProps())
          val topicKey = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
          val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
          val schema = Seq(Column.builder().name("a").dataType(DataType.STRING).order(1).build())

          Await.result(workerClient
            .connectorCreator()
            .topicKey(topicKey)
            .connectorClass(classOf[RandomNumberSourceConnecotr])
            .numberOfTasks(1)
            .connectorKey(connectorKey)
            .settings(Map())
            .columns(schema)
            .create(), 10 seconds)

          val consumer =
            Consumer
              .builder[Row, Array[Byte]]()
              .topicName(topicKey.topicNameOnKafka)
              .offsetFromBegin()
              .connectionProps(brokers.connectionProps)
              .keySerializer(Serializer.ROW)
              .valueSerializer(Serializer.BYTES)
              .build()


          val record = consumer.poll(java.time.Duration.ofSeconds(30), 3).asScala
          record.size >= 3 shouldBe true
          println(s"Record size is: ${record.size}")
          println(s"Value is: ${record.head.key().get.cell(0).value()}")
        } finally Releasable.close(workers)
      } finally Releasable.close(brokers)
    } finally Releasable.close(zookeeper)
  }
}
