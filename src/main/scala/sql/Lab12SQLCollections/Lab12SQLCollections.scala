package sql.Lab12SQLCollections

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import sql.schemas.Schemas.{customersSchema, ordersInfoSchema, ordersSchema}

/*
* Lab12 - пример использования explode и struct
* Необходимо вывести историю выполненных заказов для клиента c именем John
* Итоговое множество содержит поля: customer.name, effective_date, expiration_date, status
*/

class Lab12SQLCollections(ordersFilePath: String,
                          customersFilePath: String,
                          ordersInfoFilePath: String)(implicit sqlContext: SQLContext) {

  def job(ordersFilePath: String = ordersFilePath,
          customersFilePath: String = customersFilePath,
          ordersInfoFilePath: String = ordersInfoFilePath): Unit = {

    val orders = getOrdersDf(ordersFilePath)
    val customers = getCustomersDf(customersFilePath)
    val ordersInfo = getOrdersInfoDf(ordersInfoFilePath)

    val filteredOrders = orders
      .filter(column("status").equalTo("delivered"))
      .join(broadcast(customers), orders("customer_id") === customers("id"))
      .filter(column("name").equalTo("John"))

    val infoByDatesAndStatus = ordersInfo
      .withColumn("attributes", concat(
        array(
          struct(
            column("departure_date").as("effective_date"),
            column("transfer_date").as("expiration_date"),
            lit("transfer").as("status"))),
        array(
          struct(
            column("transfer_date").as("effective_date"),
            column("delivery_date").as("expiration_date"),
            lit("delivered").as("status")))
        )
      )

    filteredOrders.join(infoByDatesAndStatus, filteredOrders("order_id") === infoByDatesAndStatus("id"))
      .select(
        column("name"),
        explode(column("attributes")).as("attributes")
      )
      .withColumn("effective_date", column("attributes").getItem("effective_date"))
      .withColumn("expiration_date", column("attributes").getItem("expiration_date"))
      .withColumn("status", column("attributes").getItem("status"))
      .drop("attributes")
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

  private def getOrdersInfoDf(ordersInfoFilePath: String): DataFrame = {
    sqlContext.read
      .options(Map("sep" -> "\t"))
      .schema(ordersInfoSchema)
      .csv(path = ordersInfoFilePath)
  }

}

object Lab12SQLCollections {
  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Lab12SQLCollections")
    .getOrCreate()
  implicit val sqlContext: SQLContext = sparkSession.sqlContext
}

object Test {
  def main(args: Array[String]): Unit = {
    val ordersFilePath = ""
    val customersFilePath = ""
    val ordersInfoFilePath = ""

    import Lab12SQLCollections.sqlContext
    new Lab12SQLCollections(ordersFilePath, customersFilePath, ordersInfoFilePath).job()
  }
}
