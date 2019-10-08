package idv.jack.ohara

import java.sql.DriverManager

import com.island.ohara.client.configurator.v0.QueryApi.RdbColumn
import com.island.ohara.client.database.DatabaseClient
import com.island.ohara.client.kafka.WorkerClient
import com.island.ohara.common.data.{Column, DataType, Row, Serializer}
import com.island.ohara.common.setting.{ConnectorKey, TopicKey}
import com.island.ohara.common.util.CommonUtils
import com.island.ohara.kafka.Consumer
import com.island.ohara.testing.service.{Brokers, Database, Workers, Zookeepers}
import org.junit.Test
import org.scalatest.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.Await

class TestJDBCSinkConnector extends Matchers {

  @Test
  def test(): Unit = {
    val db = Database.local()
    val client = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
    val tableName = "table1"
    val column1 = RdbColumn("a", "VARCHAR(45)", true)
    val column2 = RdbColumn("b", "VARCHAR(45)", false)
    client.createTable(tableName, Seq(column1, column2))

    val brokerNumber = 1
    val workerNumber = 1
    val zookeeper = Zookeepers.local(0)
    val brokers = Brokers.local(zookeeper, (1 to brokerNumber).map(x => 0).toArray)
    val workers = Workers.local(brokers, (1 to workerNumber).map(x => 0).toArray)

    val workerClient = WorkerClient(workers.connectionProps())
    val topicKey = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey1 = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey2 = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

    val schema = Seq(Column.builder().name("a").dataType(DataType.STRING).order(1).build(),
                     Column.builder().name("b").dataType(DataType.STRING).order(1).build())

    //Start RandomNumberSourceConnector
    Await.result(workerClient
      .connectorCreator()
      .topicKey(topicKey)
      .connectorClass(classOf[RandomNumberSourceConnecotr])
      .numberOfTasks(1)
      .connectorKey(connectorKey1)
      .settings(Map())
      .columns(schema)
      .create(), 10 seconds)

    //Start JDBCSinkConnector
    Await.result(workerClient
      .connectorCreator()
      .topicKey(topicKey)
      .connectorClass(classOf[JDBCSinkConnector])
      .numberOfTasks(1)
      .connectorKey(connectorKey2)
      .settings(Map("db.tablename" -> tableName,
                    "db.url" -> db.url,
                    "db.username" -> db.user,
                    "db.password" -> db.password
      ))
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

    //Check database data
    val connection = client.connection
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(s"SELECT * FROM $tableName")
    var count = 0
    while(resultSet.next()) {
      count = count + 1
    }
    count >= 3 shouldBe true
    workers.close()
    brokers.close()
    zookeeper.close()
    statement.close()
    connection.close()
    client.close()
  }

}
