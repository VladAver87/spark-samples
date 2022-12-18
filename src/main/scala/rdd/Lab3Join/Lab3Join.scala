package rdd.Lab3Join

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import rdd.DataModels.{Customer, Order, Product}

import java.sql.Date
import scala.util.{Failure, Success, Try}

object Lab3Join {
  /*
   * Lab3 - пример использования join
   * Расчитать кто и сколько сделал заказов, за какие даты, на какую сумму.
   * Итоговое множество содержит поля: customer.name, order.order.orderDate, sum(order.numberOfProduct * product.price)
   * */

  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Lab3Join")
  val sc = new SparkContext(sparkConf)
  val customersFailAcc: LongAccumulator = sc.longAccumulator
  val ordersFailAcc: LongAccumulator = sc.longAccumulator
  val productsFailAcc: LongAccumulator = sc.longAccumulator
  val customersFilePath = ""
  val ordersFilePath = ""
  val productsFilePath = ""


  def job(customersFilePath: String, ordersFilePath: String, productsFilePath: String): Unit = {

    val customers = getCustomers(customersFilePath = customersFilePath)
      .map(customer => customer.id -> customer.name)

    val orders = getOrders(ordersFilePath = ordersFilePath)
      .map(order => order.customerId -> (order.orderId, order.productId, order.orderDate, order.numberOfProducts))

    val products = getProducts(productsFilePath = productsFilePath)
      .map(product => product.id -> product.price)

    orders.join(customers).flatMap {
      case (_, ((_, prodId, ordDate, numOfProd), custName)) => Some(prodId -> (ordDate, numOfProd, custName))
      case _ => None
    }
      .join(products).flatMap {
      case (_, ((ordDate, numOfProd, custName), prodPrice)) => Some(custName -> ordDate, numOfProd * prodPrice)
      case _ => None
    }
      .reduceByKey((totalPrice1, totalPrice2) => totalPrice1 + totalPrice2)
      .map { case ((custName, ordDate), totalPrice) => (custName, ordDate, totalPrice) }
      .sortBy(_._1)
      .foreach { case (custName, ordDate, totalPrice) =>
        println(s"customer_name: $custName, order_date: $ordDate, total_price: $totalPrice")
      }

    println(s"Total of failed rows while parsing customers data: ${customersFailAcc.count}")
    println(s"Total of failed rows while parsing orders data: ${ordersFailAcc.count}")
    println(s"Total of failed rows while parsing products data: ${productsFailAcc.count}")
  }


  private def getCustomers(customersFilePath: String,
                            separator: String = "\t"
                          ): RDD[Customer] = sc.textFile(customersFilePath).flatMap {
    _.split(separator, -1) match {
      case Array(id, name, email, joinDate, status) => Try {
        Customer(
          id = id.toInt,
          name = name,
          email = email,
          joinDate = Date.valueOf(joinDate),
          status = status
        )
      } match {
        case Success(customer) => Some(customer)
        case Failure(_) =>
          customersFailAcc.add(1)
          None
      }
      case _ => None
    }
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
      } match {
        case Success(order) => Some(order)
        case Failure(_) =>
          ordersFailAcc.add(1)
          None
      }
      case _ => None
    }
  }

  private def getProducts(productsFilePath: String,
                          separator: String = "\t"
                         ): RDD[Product] = sc.textFile(productsFilePath).flatMap {
    _.split(separator, -1) match {
      case Array(id, name, price, numberOfProducts) => Try {
        Product(
          id = id.toInt,
          name = name,
          price = price.toDouble,
          numberOfProducts = numberOfProducts.toInt
        )
      } match {
        case Success(product) => Some(product)
        case Failure(_) =>
          productsFailAcc.add(1)
          None
      }
      case _ => None
    }
  }

  def main(args: Array[String]): Unit = {
    job(
      customersFilePath = customersFilePath,
      ordersFilePath = ordersFilePath,
      productsFilePath = productsFilePath
    )
  }
}
