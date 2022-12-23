package rdd.Lab1ReduceByKey

import org.apache.spark.{SparkConf, SparkContext}
import rdd.model.Order

import java.sql.Date
import scala.util.Try

object Lab1ReduceByKey {
  /*
   * Lab1 - пример использование reduceByKey
   * Посчитать общий объем всех заказов и какое кол-во раз был выполнен заказ,
   * относительно каждого клиента.
   * Итоговое множество содержит поля: order.customerID, sum(order.numberOfProduct), count(1)
   * */
//  val ordersFilePath = ""
//  val spark = SparkSession.builder()
//    .master("local")
//    .appName("Lab1ReduceByKey")
//    .getOrCreate()
//
//
//  spark.sparkContext.textFile(path = ordersFilePath)
//    .map(data => data.split('\t'))
//    .map(line => Order(line.head.toInt, line(1).toInt, line(2).toInt, line(3).toInt, line(4), line(5)))
//    .map(item => item.customerId -> (item.numberOfProducts, if (item.status == "delivered") 1 else 0))
//    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
//    .map{ case (customerId, (numOfProd, sucOrdersCount)) => (customerId, numOfProd, sucOrdersCount)}
//    .sortBy(_._1)
//    .foreach(println)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Lab1ReduceByKey")
    val sc = new SparkContext(sparkConf)

    val orderFilePath = ""

    sc.textFile(orderFilePath).flatMap {
      _.split("\t") match {
        case Array(custId, id, productId, numberOfProducts, orderDateStr, status) if status == "delivered" => Try {
          Order(
            customerId = custId.toInt,
            orderId = id.toInt,
            productId = productId.toInt,
            numberOfProducts = numberOfProducts.toInt,
            orderDate = Date.valueOf(orderDateStr),
            status = status
          )
        }.toOption
        case _ => None
      }
    }
      .map(order => (order.customerId, (order.numberOfProducts, 1)))
      .reduceByKey( (numProducts, count) =>
        (numProducts._1 + numProducts._2, count._1 + count._2)
      )
      .sortBy(_._1)
      .foreach { case (custId, (numberOfProd, count)) =>
        println(s"id: $custId, products number: $numberOfProd, count_delivered: $count")
      }
    sc.stop()
  }

}