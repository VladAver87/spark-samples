package sql.Lab11WindowFunction

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, column, rank, sum}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import sql.schemas.Schemas.{customersSchema, ordersSchema, productsSchema}

/*
 * Lab11 - пример использования оконных выражений
 * Необходимо определить самый популярный продукт у клиента
 * Итоговое множество содержит поля: customer.name, product.name
 * */

class Lab11WindowFunction(ordersFilePath: String,
                          customersFilePath: String,
                          productsFilePath: String)(implicit sqlContext: SQLContext) {

  def job(ordersFilePath: String = ordersFilePath,
          customersFilePath: String = customersFilePath,
          productsFilePath: String = productsFilePath): Unit = {

    val orders = getOrdersDf(ordersFilePath)
    val customers = getCustomersDf(customersFilePath)
    val products = getProductsDf(productsFilePath)

    val windowSpec = Window
      .partitionBy(column("customer_id"), column("product_id"))
      .orderBy(column("number_of_products"))
    val windowSpecWithSum = Window
      .partitionBy(column("customer_id"))
      .orderBy(column("total").desc)

    orders
      .withColumn("total", sum("number_of_products").over(windowSpec))
      .withColumn("rank", rank().over(windowSpecWithSum))
      .join(broadcast(customers), orders("customer_id") === customers("id"))
      .withColumnRenamed("name", "customer_name")
      .join(broadcast(products), orders("product_id") === products("id"))
      .withColumnRenamed("name", "product_name")
      .select("customer_name", "product_name").distinct()
      .where(column("rank") === 1)
      .show(truncate = false)

  }

  private def getOrdersDf(ordersFilePath: String): DataFrame = {
    sqlContext.read
      .options(Map("sep" -> "\t"))
      .schema(ordersSchema)
      .csv(path = ordersFilePath)
  }

  private def getCustomersDf(customersFilePath: String): DataFrame = {
    sqlContext.read
      .options(Map("sep" -> "\t"))
      .schema(customersSchema)
      .csv(path = customersFilePath)
  }

  private def getProductsDf(productsFilePath: String): DataFrame = {
    sqlContext.read
      .options(Map("sep" -> "\t"))
      .schema(productsSchema)
      .csv(path = productsFilePath)
  }

}

object Lab11WindowFunction {
  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Lab11WindowFunction")
    .getOrCreate()
  implicit val sqlContext: SQLContext = sparkSession.sqlContext
}

object Test {
  def main(args: Array[String]): Unit = {
    val ordersFilePath = ""
    val customersFilePath = ""
    val productsFilePath = ""

    import Lab11WindowFunction.sqlContext
    new Lab11WindowFunction(ordersFilePath, customersFilePath, productsFilePath).job()
  }
}
