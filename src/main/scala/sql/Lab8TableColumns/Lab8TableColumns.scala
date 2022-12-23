package sql.Lab8TableColumns

import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Column, SQLContext, SparkSession}

/*
 * Lab8 - пример обработки полей при помощи select/withColumns
 * Вывести название всех устройств,
 * если цена больее 50000 вычесть 10% от стоимости назвать поле new_price,
 * добавить поле type, используя функцию getTypeDevice
 * Итоговое множество содержит поля: product.name, new_price, type
 * */

class Lab8TableColumns(productsFilePath: String)(implicit sqlContext: SQLContext) {

  def job(productsFilePath: String = productsFilePath): Unit = {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("price", DoubleType)
      .add("number_of_products", IntegerType)
    sqlContext.read
      .options(Map("sep" -> "\t"))
      .schema(schema)
      .csv(path = productsFilePath)
      .withColumn("new_price",
        when(col("price") > 50000, col("price") - (col("price") / 10))
          .otherwise(""))
      .withColumn("type", getTypeDevice(col("name")))
      .select("name", "new_price", "type")
      .show(truncate = false)
  }

  private def getTypeDevice(column: Column): Column = when(column.rlike("iPhone"), lit("phone"))
    .when(column.rlike("iPad"), lit("tablet"))
    .when(column.rlike("MacBook"), lit("pc"))
    .when(column.rlike("HomePod"), lit("smart pod"))
    .when(column.rlike("AirPods || EarPods"), lit("headphones"))
}

object Lab8TableColumns {
  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Lab8TableColumns")
    .getOrCreate()
  implicit val sqlContext: SQLContext = sparkSession.sqlContext
}

object Test {
  def main(args: Array[String]): Unit = {
    val productsFilePath = "/Users/vladislav/Documents/task_spark/dataset/product/product.csv"

    import Lab8TableColumns.sqlContext
    new Lab8TableColumns(productsFilePath).job()
  }
}
