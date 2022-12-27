package rdd.model

import java.sql.Date

case class Order(
                  customerId: Int,
                  orderId: Int,
                  productId: Int,
                  numberOfProducts: Int,
                  orderDate: Date,
                  status: String
                )
