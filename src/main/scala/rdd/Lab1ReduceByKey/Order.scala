package rdd.Lab1ReduceByKey

case class Order(
                  customerId: Int,
                  orderId: Int,
                  productId: Int,
                  numberOfProducts: Int,
                  orderDate: String,
                  status: String
                )
