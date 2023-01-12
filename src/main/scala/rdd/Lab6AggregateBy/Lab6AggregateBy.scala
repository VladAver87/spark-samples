package rdd.Lab6AggregateBy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import rdd.model.DataModels.Order

import java.sql.Date
import scala.util.Try

/*
 * Lab6 - пример использования aggregateByKey
 * Определить кол-во уникальных заказов, максимальный объем заказа,
 * минимальный объем заказа, общий объем заказа за всё время
 * Итоговое множество содержит поля: order.customerID, count(distinct order.productID),
 * max(order.numberOfProduct), min(order.numberOfProduct), sum(order.numberOfProduct)
 * */

class Lab6AggregateBy(ordersFilePath: String)(implicit sc: SparkContext) {

  def job(ordersFilePath: String = ordersFilePath): Unit = {

    getOrders(ordersFilePath = ordersFilePath)
      .map(order => order.customerId -> (order.productId, order.numberOfProducts))
      .aggregateByKey((Set[Int](), 0, 0, 0))({ case ((orders, maxNumOfProd, minNumOfProd, sumNumOfProd), (prodId, numOfProd)) =>
        val newOrders = orders + prodId
        val max = if (maxNumOfProd > numOfProd) maxNumOfProd else numOfProd
        val min = if (minNumOfProd < numOfProd && minNumOfProd != 0) minNumOfProd else numOfProd
        val sum = sumNumOfProd + numOfProd
        (newOrders, max, min, sum)
      },
        { case ((orders1, maxNumOfProd1, minNumOfProd1, sumNumOfProd1), (orders2, maxNumOfProd2, minNumOfProd2, sumNumOfProd2)) =>
          val max = if (maxNumOfProd1 > maxNumOfProd2) maxNumOfProd1 else maxNumOfProd2
          val min = if (minNumOfProd1 < minNumOfProd2) minNumOfProd1 else minNumOfProd2
          val sum = sumNumOfProd1 + sumNumOfProd2
          (orders1 ++ orders2, max, min, sum)
        })
      .map { case (custId, (orders, max, min, sum)) => custId -> (orders.size, max, min, sum) }
      .foreach { case (custId, (orders, max, min, sum)) =>
        println(s"customer_id: $custId, count_distinct_products: $orders, " +
          s"max_num_of_product: $max, min_num_of_product: $min, sum_num_of_product: $sum")
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

object Lab6AggregateBy {
  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Lab6AggregateBy")
  implicit val sc: SparkContext = new SparkContext(sparkConf)
}

object Test {
  def main(args: Array[String]): Unit = {
    val ordersFilePath = ""

    import Lab6AggregateBy.sc
    new Lab6AggregateBy(ordersFilePath).job(ordersFilePath = ordersFilePath)
  }
}
