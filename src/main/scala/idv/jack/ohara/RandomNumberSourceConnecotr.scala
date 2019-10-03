package idv.jack.ohara

import java.util

import com.island.ohara.common.setting.SettingDef
import com.island.ohara.kafka.connector.{RowSourceConnector, RowSourceTask, TaskSetting}

import scala.collection.JavaConverters._

class RandomNumberSourceConnecotr extends RowSourceConnector {
  private[perf] var settings: TaskSetting = _

  override def _taskClass(): Class[_ <: RowSourceTask] = classOf[RandomNumberTask]

  override def _taskSettings(maxTasks: Int): util.List[TaskSetting] =
    Seq.fill(maxTasks)(settings).asJava

  override def _start(config: TaskSetting): Unit = {
    this.settings = settings
  }

  override def _stop(): Unit = {
    //Nothing
  }

  override protected def _definitions(): java.util.List[SettingDef] =
    Seq().asJava
}
