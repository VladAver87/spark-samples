package rdd.Lab5GroupBy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import rdd.DataModels.Order

import java.sql.Date
import scala.util.Try

/*
* Lab5 - пример использования groupByKey
* Определить средний объем заказа, за всё время, для каждого заказчика
* Итоговое множество содержит поля: order.customerID, sum(order.numberOfProduct),
* count(order.numberOfProduct), sum(order.numberOfProduct) / count(order.numberOfProduct)
* */

class Lab5GroupBy(ordersFilePath: String)(implicit sc: SparkContext) {

  def job(ordersFilePath: String = ordersFilePath): Unit = {

    getOrders(ordersFilePath = ordersFilePath)
      .map(order => order.customerId -> order.numberOfProducts)
      .groupByKey()
      .map{ case (custId, numbersOfProd) =>
        custId -> (numbersOfProd.sum, numbersOfProd.size, numbersOfProd.sum.toDouble / numbersOfProd.size)}
      .sortBy(_._1)
      .foreach { case (custId, (sumNumOfProd, countNumOfProd, avgNumOfProd)) =>
        println(s"customer_id: $custId, sum_num_of_product: $sumNumOfProd, " +
          s"count_num_of_product: $countNumOfProd, avg_num_of_prod: $avgNumOfProd")
      }

    sc.stop()
  }

  private def getOrders(ordersFilePath: String,
                        separator: String = "\t"
                       ): RDD[Order] = sc.textFile(ordersFilePath).flatMap {
    _.split(separator, -1) match {
      case Array(custId, orderId, productId, numberOfProducts, orderDateStr, status) => Try {
        Order(
          customerId = custId.toInt,
          orderId = orderId.toInt,
          productId = productId.toInt,
          numberOfProducts = numberOfProducts.toInt,
          orderDate = Date.valueOf(orderDateStr),
          status = status
        )
      }.toOption
      case _ => None
    }
  }
}

object Lab5GroupBy {
  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Lab5GroupBy")
  implicit val sc: SparkContext = new SparkContext(sparkConf)
}

object Test {
  def main(args: Array[String]): Unit = {
    val ordersFilePath = "/Users/vladislav/Documents/task_spark/dataset/order/order.csv"

    import Lab5GroupBy.sc
    new Lab5GroupBy(ordersFilePath).job(ordersFilePath = ordersFilePath)
  }
}