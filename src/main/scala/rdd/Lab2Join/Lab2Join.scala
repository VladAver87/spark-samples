package rdd.Lab2Join

import org.apache.spark.sql.SparkSession
import rdd.model.{Order, Product}

object Lab2Join extends App {
  /*
 * Lab2 - пример использования leftOuterJoin
 * Определить продукты, которые ни разу не были заказаны
 * Итоговое множество содержит поле product.name
 * */

  val ordersFilePath = "/Users/vladislav/Documents/task_spark/dataset/order/order.csv"
  val productsFilePath = "/Users/vladislav/Documents/task_spark/dataset/product/product.csv"

  val spark = SparkSession.builder()
    .master("local")
    .appName("Lab2Join")
    .getOrCreate()


  val ordersRdd = spark.sparkContext.textFile(path = ordersFilePath)
    .map(data => data.split('\t'))
    .map(line => Order(line.head.toInt, line(1).toInt, line(2).toInt, line(3).toInt, line(4), line(5)))
    .map(order => order.productId -> "")
    .distinct()

  val productsRdd = spark.sparkContext.textFile(path = productsFilePath)
    .map(data => data.split('\t'))
    .map(line => Product(line.head.toInt, line(1), line(2).toInt, line(3).toInt))
    .map(product => product.id -> product)

  productsRdd.leftOuterJoin(ordersRdd)
    .filter(_._2._2.isEmpty)
    .map(item => item._2._1.name)
    .foreach(println)
}
