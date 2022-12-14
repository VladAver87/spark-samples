package rdd.Lab1ReduceByKey

import org.apache.spark.sql.SparkSession

object Lab1ReduceByKey extends App {
  /*
   * Lab1 - пример использование reduceByKey
   * Посчитать общий объем всех заказов и какое кол-во раз был выполнен заказ,
   * относительно каждого клиента.
   * Итоговое множество содержит поля: order.customerID, sum(order.numberOfProduct), count(1)
   * */
  val ordersFilePath = "/Users/vladislav/Documents/task_spark/dataset/order/order.csv"
  val spark = SparkSession.builder()
    .master("local")
    .appName("Lab1ReduceByKey")
    .getOrCreate()


  val ordersRdd = spark.sparkContext.textFile(path = ordersFilePath)
    .map(data => data.split('\t'))
    .map(line => Order(line.head.toInt, line(1).toInt, line(2).toInt, line(3).toInt, line(4), line(5)))

  val numberOfProductsRdd = ordersRdd
    .map(item => (item.customerId, item.numberOfProducts))
    .reduceByKey(_ + _)

  val successOrdersRdd = ordersRdd
    .filter(_.status == "delivered")
    .map(item => (item.customerId, 1))
    .reduceByKey(_ + _)

  numberOfProductsRdd.join(successOrdersRdd)
    .map{ case (customerId, (numOfProd, sucOrdersCount)) => (customerId, numOfProd, sucOrdersCount)}
    .foreach(println)
}