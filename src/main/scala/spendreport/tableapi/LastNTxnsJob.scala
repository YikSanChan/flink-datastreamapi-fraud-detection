package spendreport.tableapi

import com.typesafe.config.ConfigFactory
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource

object LastNTxnsJob {

  final case class Row(accountId: Long, lastNClicks: String)

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val query = config.getString("last-n-txns.query")

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    val txnStream: DataStream[Transaction] = streamEnv
      .addSource(new TransactionSource)
      .name("transactions")

    tableEnv.createTemporaryView("transactions", txnStream)

    tableEnv.executeSql(query).print()
  }
}
