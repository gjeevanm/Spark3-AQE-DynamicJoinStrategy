package com.gjeevanm.spark3

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec}
import com.gjeevanm.spark3.AQEHelperClass._

object DynamicJoinStrategy extends App {

  val sparkconf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Dynamic Join Startegy")

  val spark = SparkSession
    .builder()
    .config(sparkconf)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
    .config("spark.sql.adaptive.logLevel", "TRACE")
    .getOrCreate()


  println("************************************************* " + spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

  val df1 = spark.sql(
    """
      |SELECT id AS i_item_id,
      |CAST(rand() * 1000 AS INT) AS i_price
      |FROM RANGE(30000000)
      |""".stripMargin
  )

  df1.createOrReplaceTempView("items")

  val df2 = spark.sql(
    """
      |SELECT CASE WHEN rand() < 0.8 THEN 100 ELSE CAST(rand() * 30000000 AS INT) END AS s_item_id,
      |CAST(rand() * 100 AS INT) AS s_quantity,
      |DATE_ADD(current_date(), - CAST(rand() * 360 AS INT)) AS s_date
      |FROM RANGE(10000000)
      |""".stripMargin
  )

  df2.createOrReplaceTempView("sales")

  val dfAdaptive = spark.sql(
    """
      |SELECT s_date, sum(s_quantity * i_price) AS total_sales
      |FROM sales
      |JOIN items ON s_item_id = i_item_id
      |WHERE i_price < 10
      |GROUP BY s_date
      |ORDER BY total_sales DESC
      |""".stripMargin)

  // Plan before execution of join - Sort Merge Join
  val planbefore = dfAdaptive.queryExecution.executedPlan

  val shortMergeJoinSize = findTopLevelSortMergeJoin(planbefore).size

  val result = dfAdaptive.collect()

  // Plan after execution of join - BroadCastHashJoin
  val planAfter = dfAdaptive.queryExecution.executedPlan

  val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan

  val broadcastJoinSize = findTopLevelBroadcastHashJoin(adaptivePlan).size

  dfAdaptive.show(100,false)

}
