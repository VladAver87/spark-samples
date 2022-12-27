package sql.Lab9ProcessingTables

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import sql.schemas.Schemas.{customersSchema, ordersSchema, productsSchema}

import java.sql.Date

/*
 * Lab9 - пример использования filter, join, crossJoin, broadcast, orderBy
 * Вывести информацию о клиенте email, название продукта
 * и кол-во доставленного товара за первую половину 2018 года
 * Итоговое множество содержит поля: customer.email, product.name, order.order_date, order.number_of_product
 * */

class Lab9ProcessingTables(ordersFilePath: String,
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
      .select("email", "product_name", "order_date", "ord_number_of_products")
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

object Lab9ProcessingTables {
  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Lab9ProcessingTables")
    .getOrCreate()
  implicit val sqlContext: SQLContext = sparkSession.sqlContext
}

object Test {
  def main(args: Array[String]): Unit = {
    val ordersFilePath = ""
    val customersFilePath = ""
    val productsFilePath = ""

    import Lab9ProcessingTables.sqlContext
    new Lab9ProcessingTables(ordersFilePath, customersFilePath, productsFilePath).job()
  }
}
