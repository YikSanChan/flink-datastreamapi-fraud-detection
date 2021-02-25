package spendreport.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.walkthrough.common.entity.{Alert, Transaction}
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.source.TransactionSource

object FraudDetectionJob {

  final case class Row(accountId: Long)

  final val FRAUD_DETECTOR_QUERY =
    """
      |SELECT accountId FROM transactions
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    val txnStream: DataStream[Transaction] = streamEnv
      .addSource(new TransactionSource)
      .name("transactions")

    tableEnv.createTemporaryView("transactions", txnStream)

    val fraudTxnTable = tableEnv.sqlQuery(FRAUD_DETECTOR_QUERY)

    val alertStream = tableEnv
      .toAppendStream[Row](fraudTxnTable)
      .map(row => {
        val alert = new Alert
        alert.setId(row.accountId)
        alert
      })

    alertStream
      .addSink(new AlertSink)
      .name("send-alerts")

    streamEnv.execute("Fraud Detection")
  }
}
