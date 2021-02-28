package spendreport.tableapi

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource

object LastNTxnsJob {

  final val QUERY =
    """
      |WITH last_n AS (
      |    SELECT accountId, `timestamp`
      |    FROM (
      |        SELECT *,
      |            ROW_NUMBER() OVER (PARTITION BY accountId ORDER BY `timestamp` DESC) AS row_num
      |        FROM transactions
      |    )
      |    WHERE row_num <= 2
      |)
      |SELECT accountId, LISTAGG(CAST(`timestamp` AS STRING)) last2_timestamp
      |FROM last_n
      |GROUP BY accountId
      |""".stripMargin

  final case class Row(accountId: Long, lastNClicks: String)

  def main(args: Array[String]): Unit = {
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    val txnStream: DataStream[Transaction] = streamEnv
      .addSource(new TransactionSource)
      .name("transactions")

    tableEnv.createTemporaryView("transactions", txnStream)

    tableEnv.executeSql(QUERY).print()
  }
}
