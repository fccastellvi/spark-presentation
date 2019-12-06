package com.ferran.projects

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

case class AddressAggregate(address: String, total_incoming: Long, total_outgoing: Long)

case class FlowAggregate(from: String, to: String, total_flow: Long)

case class ValueTransferEvent(from: String, to: String, value: Long)

object ScalaPresentation extends App {


  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val badActors = Seq(
    ("Joan", "Marc", 10),
    ("Pau", "Burni", 20),
    ("Carla", "Xavier", 500),
    ("Albert", "Gerard", 500000000),
    ("Victor", "Ferran", 90),
    ("Carlota", "Guillem", 60),
    ("Cristina", "Xavier", 60))
    .toDF("from", "to", "value")
    .as[ValueTransferEvent]

  val goodActors = Seq(
    ("Carla", "Gerard", 10),
    ("Burni", "Marc", 20),
    ("Victor", "Cristina", 500),
    ("Alberto", "Ferran", 500000000),
    ("Burni", "Guillem", 20),
    ("David", "Irene", 60))
    .toDF("from", "to", "value")
    .as[ValueTransferEvent]

  val function: String => String = _ + " BAD"

  val myudf = udf(function)

  badActors.withColumn("Bad", myudf($"from")).show

  badActors.groupBy("from").agg(sum($"value")).show
  badActors.groupBy("to").agg(sum($"value")).show

  val bigBlock = spark.read.json("/Users/ferrancabezas/repos/PET/spark-presentation/Data/*")

  val tx1 = bigBlock.select(explode($"transactions")).select($"col.*")
  val tx2 = bigBlock.select(explode($"transactions")).select($"col.*")

  val joinedWithCache = tx1.join(tx2, Seq("from")).cache()
  val joinedWithoutCache = tx1.join(tx2, Seq("to"))
  joinedWithCache.show(20)
  joinedWithoutCache.show(20)
}
