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

package com.archerial.samples
import com.archerial._

object SampleData{
  val tables = Tables
  def createTables(implicit con:java.sql.Connection) {
	Tables.create_all()
  }
  def insertSampleData(implicit con:java.sql.Connection) {
	import anorm._ 
	import com.archerial._
	import RawValImplicits._
	import RawVal.Null
	// Tables.staff.insertRows(
	//   List("age"-> 15, "name"-> "hokari", "dept_id" -> 1, "boss_id" -> Null,"height" -> 170),
	//   List("age"-> 16, "name"-> "mikio", "dept_id" -> 1, "boss_id" -> 1,"height" -> 160),
	//   List("age"-> 17, "name"-> "keiko", "dept_id" -> 2, "boss_id" -> 2,"height" -> 150))

    Tables.staff.insertRows(
      List("id"-> 1, "name"-> "Guido", "boss_id" -> Null,
		   "height" -> 170, "sex" -> "male"),
      List("id"-> 2, "name"-> "Martin", "boss_id" -> 1,
		   "height" -> 160, "sex" -> "male"),
      List("id"-> 3, "name"-> "Larry", "boss_id" -> 2,
		   "height" -> 150, "sex" -> "male")
      ,List("id"-> 4, "name"-> "Rich", "boss_id" -> 1,
		   "height" -> 180, "sex" -> "female")
    )

	Tables.dept.insertRows(
	  List("name"-> "kaihatu", "area_id" -> 1 ),
	  List("name"-> "kenkyu",  "area_id" -> 1))
	Tables.area.insertRows(List("name"-> "Tokyo"))
	Tables.hobby.insertRows(
	  List("staff_id"-> 1, "name"-> "piano", "rank" -> "A" ),
	  List("staff_id"-> 1, "name"-> "piano", "rank" -> "B" ),
	  List("staff_id"-> 1, "name"-> "tennis", "rank" -> "A"),
	  List("staff_id"-> 3, "name"-> "piano", "rank" -> "A")
	)

	Tables.staffInfo.insertRows(
	  List("info"-> "hokariInfo")
	)
	
	Tables.product.insertRows(
	  List("name"-> "apple", "price" -> 150),
	  List("name"-> "orange", "price" -> 100),
	  List("name"-> "melon", "price" -> 500))

	Tables.order.insertRows(
	  List("memo"-> "AA", "staff_id" -> 2),
	  List("memo"-> "BB", "staff_id" -> 2),
	  List("memo"-> "CC", "staff_id" -> 3),
	  List("memo"-> "DD", "staff_id" -> RawVal.Null)
	)

	Tables.orderItem.insertRows(
	  List("order_id"-> 1,"product_id"-> 1,"qty" -> 3),
	  List("order_id"-> 1,"product_id"-> 3,"qty" -> 2),
	  List("order_id"-> 2,"product_id"-> 3,"qty" -> 1),
	  List("order_id"-> 3,"product_id"-> 2,"qty" -> 5)
	)


  }  
}
