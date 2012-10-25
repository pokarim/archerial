/**
 * Copyright 2012 Mikio Hokari
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  */

package com.archerial.tests

import org.specs2.mutable._ 

import com.archerial._
import com.archerial.arrows._
import com.archerial.objects._
import com.archerial.queryexp._
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._

import com.archerial.samples.{SampleData,Tables,Mappers}
import com.archerial.samples.Mappers._
import java.sql.{ Array => _, _ }
import javax.sql._
import ValueImpilcits._
import RawVal._
import RawValImplicits._
import ColType._


import com.archerial.exts.ToJson._
import com.codahale.jerkson.AST._//JValue
import com.codahale.jerkson.Json.{generate,parse}
import com.codahale.jerkson.Json

import com.fasterxml.jackson.databind.{MappingJsonFactory, ObjectMapper}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser => JacksonParser}



class OrderSampleSpec extends Specification {

  "Sample 1" should {
    val h2driver = Class.forName("org.h2.Driver")
    implicit val con:java.sql.Connection = DriverManager.getConnection("jdbc:h2:mem:ordersamplespec", "", "")
	object Tables{
	  val order = Table("orders", List(
		Column("id", ColType.int.primaryKey.autoIncrement),
		Column("memo", ColType.varchar(200)),
		Column("staff_id", ColType.int)
	  ))
	  
	  val orderItem = Table("orderItem", List(
		Column("id", ColType.int.primaryKey.autoIncrement),
		Column("order_id", ColType.int),
		Column("product_id", ColType.int),
		Column("qty", ColType.int)
	  ))
	  
	  val product = Table("product", List(
		Column("id", ColType.int.primaryKey.autoIncrement),
		Column("name", ColType.varchar(200)),
		Column("price", ColType.int)
	  ))
	}
	Tables.order.createTable()
	Tables.orderItem.createTable()
	Tables.product.createTable()

	Tables.product.insertRows(
	  List("name"-> "apple", "price" -> 150),
	  List("name"-> "orange", "price" -> 100),
	  List("name"-> "melon", "price" -> 500))

	Tables.order.insertRows(
	  List("memo"-> "AA", "staff_id" -> 2),
	  List("memo"-> "BB", "staff_id" -> 2),
	  List("memo"-> "CC", "staff_id" -> 3))

	Tables.orderItem.insertRows(
	  List("order_id"-> 1,"product_id"-> 1,"qty" -> 3),
	  List("order_id"-> 1,"product_id"-> 3,"qty" -> 2),
	  List("order_id"-> 2,"product_id"-> 3,"qty" -> 1),
	  List("order_id"-> 3,"product_id"-> 2,"qty" -> 5)
	)


    val OrderId = ColObject(Tables.order,"id")
    val ProductId = ColObject(Tables.product,"id")
    val OrderItemId = ColObject(Tables.orderItem,"id")
    val orderItem2order = 
	  ColArrow(Tables.orderItem, OrderItemId, OrderId, "id", "order_id")
    val orderItem2product = 
	  ColArrow(Tables.orderItem, OrderItemId, ProductId, "id", "product_id")

	object Order extends forTableOf(Tables.order){
      val Memo = ColObject("memo")
      val memo = ColArrow(OrderId, Memo)
  	  val orderItems = ~orderItem2order
	}

    // val Id = ColObject(staffTable,"id")
    // val Name = ColObject(staffTable,"name")
    // val name = ColArrow(Id, Name)
    // val boss = ColArrow(Id, Id, "id", "boss_id")
	// val sub = ~boss
    // val staffs = AllOf(Id)
	"hoge" in { 
	  0===0
	}
  }
}
