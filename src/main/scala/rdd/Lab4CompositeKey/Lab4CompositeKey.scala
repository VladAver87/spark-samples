package rdd.Lab4CompositeKey

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import rdd.model.DataModels.{Customer, Order, Product}

import java.sql.Date
import scala.util.Try

/*
* Lab4 - пример использования cartesian и join по составному ключу
* Расчитать кто и на какую сумму купил определенного товара за всё время
* Итоговое множество содержит поля: customer.name, product.name, order.numberOfProduct * product.price
* */


class Lab4CompositeKey(customersFilePath: String,
                       ordersFilePath: String,
                       productsFilePath: String) (implicit sc: SparkContext) {

  def job(customersFilePath: String = customersFilePath,
          ordersFilePath: String = ordersFilePath,
          productsFilePath: String = productsFilePath
         ): Unit = {

    val customers = getCustomers(customersFilePath = customersFilePath)
      .map(customer => (customer.id, customer.name))

    val orders = getOrders(ordersFilePath = ordersFilePath)
      .map(order => (order.customerId, order.productId, order.numberOfProducts))

    val products = getProducts(productsFilePath = productsFilePath)
      .map(product => (product.id, product.price, product.name))

    orders.cartesian(customers)
      .filter { case ((orderCustId, _, _), (custId, _)) => orderCustId == custId }
      .map { case ((_, orderProdId, orderNumOfProd), (_, custName)) =>
        (orderProdId, custName, orderNumOfProd)
      }
      .cartesian(products)
      .filter { case ((orderProdId, _, _), (prodId, _, _)) => orderProdId == prodId }
      .map { case ((_, custName, orderNumOfProd), (_, prodPrice, prodName)) =>
        (custName, prodName) -> orderNumOfProd * prodPrice
      }
      .reduceByKey((prodPrice1, prodPrice2) => prodPrice1 + prodPrice2)
      .foreach { case (custName, prodName) -> totalPrice =>
        println(s"customer_name: $custName, product_name: $prodName, total_price: $totalPrice")
      }

    sc.stop()
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
      }.toOption
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
      }.toOption
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
      }.toOption
      case _ => None
    }
  }
}

object Lab4CompositeKey {
  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Lab4CompositeKey")
  implicit val sc: SparkContext = new SparkContext(sparkConf)
}

object Test {
  def main(args: Array[String]): Unit = {
    val customersFilePath = ""
    val ordersFilePath = ""
    val productsFilePath = ""

    import Lab4CompositeKey.sc

    new Lab4CompositeKey(customersFilePath, ordersFilePath, productsFilePath).job(
      customersFilePath = customersFilePath,
      ordersFilePath = ordersFilePath,
      productsFilePath = productsFilePath
    )
  }
}
