package idv.jack.ohara

import java.sql.{Connection, DriverManager, Statement}
import java.util
import com.island.ohara.common.data.Row
import com.island.ohara.kafka.connector.{RowSinkRecord, RowSinkTask, TaskSetting}
import scala.collection.JavaConverters._

class JDBCSinkTask extends RowSinkTask {
  private[this] var connection: Connection = _
  private[this] var statement: Statement = _
  private[this] var tableName: String = _


  override def _start(config: TaskSetting): Unit = {
    val dbURL: String = config.stringValue("db.url")
    val dbUserName: String = config.stringValue("db.username")
    val dbPassword = config.stringValue("db.password")
    tableName = config.stringValue("db.tablename")
    connection = DriverManager.getConnection(dbURL, dbUserName, dbPassword)
    statement = connection.createStatement()
  }

  override def _stop(): Unit = {
    if (statement != null) statement.close()
    if (connection != null) connection.close()
  }

  override def _put(records: util.List[RowSinkRecord]): Unit = {
    var sqlPrefix = s"INSERT INTO ${tableName}"
    records.forEach(x => {
      val row: Row = x.row()
      val columns: String = row.cells().asScala.map(x => x.name).mkString("(", ",", ")")
      val values: String = row.cells().asScala.map(x => s"'${x.value}'").mkString("(", ",", ")")
      val sql = s"$sqlPrefix $columns VALUES $values"
      statement.executeUpdate(sql)
    })
  }
}
