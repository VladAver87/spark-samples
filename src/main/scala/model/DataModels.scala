package model

import java.sql.Date

object DataModels {
  case class Customer(
                       id: Int,
                       name: String,
                       email: String,
                       joinDate: Date,
                       status: String
                     )

  case class Product(
                      id: Int,
                      name: String,
                      price: Double,
                      numberOfProducts: Int
                    )

  case class Order(
                    customerId: Int,
                    orderId: Int,
                    productId: Int,
                    numberOfProducts: Int,
                    orderDate: Date,
                    status: String
                  )
}
