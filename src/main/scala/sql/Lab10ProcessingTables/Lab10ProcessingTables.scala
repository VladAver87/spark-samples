package sql.Lab10ProcessingTables

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import sql.schemas.Schemas.{customersSchema, ordersSchema, productsSchema}

import java.sql.Date

/*
* Lab10 - пример использования join, broadcast, groupBy, agg
* Необходимо расчитать для каждого клиента,
* стоимость общей закупки каждого товара,
* максимальный объем заказанного товара,
* минимальную стоимость заказа,
* среднюю стоимость заказа
* за первую половину 2018 года, заказ должен быть доставлен
* Итоговое множество содержит поля: customer.name, product.name, sum(order.number_of_product * price),
* max(order.number_of_product), min(order.number_of_product * price), avg(order.number_of_product * price)
 */

class Lab10ProcessingTables(ordersFilePath: String,
                            customersFilePath: String,
                            productsFilePath: String)(implicit sqlContext: SQLContext) {

  def job(ordersFilePath: String = ordersFilePath,
          customersFilePath: String = customersFilePath,
          productsFilePath: String = productsFilePath): Unit = {

    val orders = getOrdersDf(ordersFilePath)
    val customers = getCustomersDf(customersFilePath)
    val products = getProductsDf(productsFilePath)

    orders
      .filter(orders("order_date").between(Date.valueOf("2018-01-01"), Date.valueOf("2018-06-31"))
        .and(orders("status").equalTo("delivered")))
      .withColumnRenamed("number_of_products", "ord_number_of_products")
      .join(broadcast(customers), orders("customer_id") === customers("id"))
      .withColumnRenamed("name", "customer_name")
      .join(broadcast(products), orders("product_id") === products("id"))
      .withColumnRenamed("name", "product_name")
      .withColumnRenamed("number_of_products", "prod_number_of_products")
      .withColumn("total_price", column("ord_number_of_products") * column("price"))
      .select("customer_name", "product_name", "total_price")
      .groupBy("customer_name", "product_name")
      .agg(
        sum("total_price").as("sum_total_price"),
        max("total_price").as("max_total_price"),
        min("total_price").as("min_total_price"),
        avg("total_price").as("avg_total_price")
      )
      .orderBy("customer_name")
      .show(numRows = 10, truncate = false)

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

object Lab10ProcessingTables {
  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Lab10ProcessingTables")
    .getOrCreate()
  implicit val sqlContext: SQLContext = sparkSession.sqlContext
}

object Test {
  def main(args: Array[String]): Unit = {
    val ordersFilePath = ""
    val customersFilePath = ""
    val productsFilePath = ""

    import Lab10ProcessingTables.sqlContext
    new Lab10ProcessingTables(ordersFilePath, customersFilePath, productsFilePath).job()
  }
}
