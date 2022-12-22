package rdd.Lab2Join

import model.DataModels.{Order, Product}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.sql.Date
import scala.util.Try

class Lab2Join(implicit sc: SparkContext) {
  /*
 * Lab2 - пример использования leftOuterJoin
 * Определить продукты, которые ни разу не были заказаны
 * Итоговое множество содержит поле product.name
 * */

//  val ordersFilePath = ""
//  val productsFilePath = ""
//
//  val spark = SparkSession.builder()
//    .master("local")
//    .appName("Lab2Join")
//    .getOrCreate()
//
//
//  val ordersRdd = spark.sparkContext.textFile(path = ordersFilePath)
//    .map(data => data.split('\t'))
//    .map(line => Order(line.head.toInt, line(1).toInt, line(2).toInt, line(3).toInt, line(4), line(5)))
//    .map(order => order.productId -> "")
//    .distinct()
//
//  val productsRdd = spark.sparkContext.textFile(path = productsFilePath)
//    .map(data => data.split('\t'))
//    .map(line => Product(line.head.toInt, line(1), line(2).toInt, line(3).toInt))
//    .map(product => product.id -> product)
//
//  productsRdd.leftOuterJoin(ordersRdd)
//    .filter(_._2._2.isEmpty)
//    .map(item => item._2._1.name)
//    .foreach(println)

  def job(orderFilePath: String, productFilePath: String): RDD[String] = {
    val products = getProducts(productFilePath).map(product => (product.id, product))

    val orders = getOrders(orderFilePath).distinct().map(order => (order.productId, order))

    products.leftOuterJoin(orders).flatMap {
      case (_, (product, orderOp)) if orderOp.isEmpty => Some(product.name)
      case _ => None
    }
  }

  private def getOrders(orderFilePath: String, separator: String = "\t"): RDD[Order] = sc.textFile(orderFilePath).flatMap {
    _.split(separator, -1) match {
      case Array(custId, id, productId, numberOfProducts, orderDateStr, status) => Try {
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


  private def getProducts(productsFilePath: String, separator: String = "\t"): RDD[Product] = sc.textFile(productsFilePath).flatMap {
    _.split(separator, -1) match {
      case Array(id, name, price, numberOfProducts) => Try {
        Product(id.toInt, name, price.toDouble, numberOfProducts.toInt)
      }.toOption
      case _ => None
    }
  }

}
