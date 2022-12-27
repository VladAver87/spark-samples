package sql.schemas

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}

object Schemas {
  val productsSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("price", DoubleType)
    .add("number_of_products", IntegerType)

  val ordersSchema: StructType = new StructType()
    .add("customer_id", IntegerType)
    .add("order_id", IntegerType)
    .add("product_id", IntegerType)
    .add("number_of_products", IntegerType)
    .add("order_date", DateType)
    .add("status", StringType)

  val ordersInfoSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("departure_date", DateType)
    .add("transfer_date", DateType)
    .add("delivery_date", DateType)
    .add("departure_city", StringType)
    .add("delivery_city", StringType)

  val customersSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("email", StringType)
    .add("join_date", DateType)
    .add("status", StringType)

}
