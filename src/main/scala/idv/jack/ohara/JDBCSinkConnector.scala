package idv.jack.ohara

import java.util

import com.island.ohara.common.setting.SettingDef

import scala.collection.JavaConverters._
import com.island.ohara.kafka.connector.{ConnectorVersion, RowSinkConnector, RowSinkTask, TaskSetting}

class JDBCSinkConnector extends RowSinkConnector {
  private[this] var config: TaskSetting = _

  override protected def _start(config: TaskSetting): Unit = {
    this.config = config
  }

  override protected def _stop(): Unit = {
    // do nothing
  }

  override protected def _taskClass(): Class[_ <: RowSinkTask] = classOf[JDBCSinkTask]

  override protected def _taskSettings(maxTasks: Int): util.List[TaskSetting] = Seq.fill(maxTasks)(config).asJava

  override protected def _version: ConnectorVersion = ConnectorVersion.DEFAULT

  override protected def _definitions(): util.List[SettingDef] = Seq(
    SettingDef
      .builder()
      .displayName("jdbc url")
      .key("db.url")
      .documentation("connection to database url")
      .valueType(SettingDef.Type.STRING)
      .build(),
    SettingDef
      .builder()
      .displayName("jdbc user name")
      .key("db.username")
      .documentation("connection to database username")
      .valueType(SettingDef.Type.STRING)
      .build(),
    SettingDef
      .builder()
      .displayName("jdbc password")
      .key("db.password")
      .documentation("connection to database password")
      .valueType(SettingDef.Type.PASSWORD)
      .build(),
    SettingDef
      .builder()
      .displayName("table name")
      .key("db.tablename")
      .documentation("insert to table")
      .valueType(SettingDef.Type.STRING)
      .build()
  ).asJava
}
