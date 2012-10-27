/**
 * Copyright 2012 Martin Guido
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

import com.archerial.samples.{SampleData,Tables,Mappers}
import com.archerial.samples.Mappers._
import java.sql.{ Array => _, _ }
import javax.sql._
import ValueImpilcits._
import RawVal._

class ArrowSpec extends Specification {

  "The 'Hello world' string" should {
	val h2driver = Class.forName("org.h2.Driver")
	implicit val con = DriverManager.getConnection("jdbc:h2:mem:hoge", "", "")
	SampleData.createTables 
	//SampleData.insertSampleData


	if(true)	
	  {
	import com.archerial._
	import RawValImplicits._
	import RawVal.Null

    Tables.staff.insertRows(
      List("id"-> 1, "name"-> "Guido", "boss_id" -> 1,
		   "height" -> 170),
      List("id"-> 2, "name"-> "Martin", "boss_id" -> 1,
		   "height" -> 160),
      List("id"-> 3, "name"-> "Larry", "boss_id" -> 2,
		   "height" -> 150)
      // ,List("id"-> 4, "name"-> "Rich", "boss_id" -> 1,
	  // 	   "height" -> 180
	  // 	 )
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
	val boss = Staff.boss
	val sub = ~boss
	val staffs = AllOf(Staff.Id)
	val name = Staff.name
	val bossname = boss >>> name
	val subname = sub >>> name
	val staffId = Staff.Id.id
	val isMartin = name =:= Const(Str("Martin"))
	val isGuido = name =:= Const(Str("Guido"))
if(true){	
	"simple arrow" in {
	  (staffs >>> name).queryExp.eval().toSet ===
		Set[Value](Str("Guido"),
				   Str("Larry"),
				   Str("Martin"))
	}
	"simple tuple" in {
	  (staffs >>> Tuple(staffId, name)).queryExp.eval().toSet === 
		Set(
		  VTuple(VList(Int(1)),
				 VList(Str("Guido"))),
		  VTuple(VList(Int(3)),
				 VList(Str("Larry"))),
		  VTuple(VList(Int(2)),
				 VList(Str("Martin"))))
	}
	"simple filter tuple" in {
	
	  (staffs >>> Filter(isMartin) >>> Tuple(
		staffId
		,name)).queryExp.eval().toSet === 
		  Set(
			VTuple(VList(Int(2)),
				   VList(Str("Martin"))))
	}
	"filter arrow" in {
	  ((staffs >>> Filter(isGuido) >>> 
		 Tuple(staffId ,name,subname)
	   ).queryExp.eval().toSet) === 
	  Set(
	   VTuple(VList(Int(1)),
			  VList(Str("Guido")),
			  VList(Str("Guido"),Str("Martin"))
			))
	}
  }
	"filter bossname subname" in {
	  // ((staffs >>> Filter(isMartin) >>> Tuple(
	  // 	staffId
	  // 	,name
	  // 	,bossname
	  // 	,subname
	  // )).queryExp.eval().toSet)
	  val q = (staffs >>> Tuple(
		staffId
		,name
		,boss >>> name
		,boss >>> Filter(Any(boss >>> name =:= name)) >>> name
	  )).queryExp

	  (staffs >>> Tuple(
		staffId
		,name
		,boss >>> name
		,boss >>> Filter(boss >>> name =:= name) >>> name
	  )).queryExp//.eval().toSet == 
		Set(
		  VTuple(VList(Int(1)),
				 VList(Str("Guido")),
				 VList(Str("Guido")),
				 VList(Str("Guido"))), 
		  VTuple(VList(Int(3)),
				 VList(Str("Larry")),
				 VList(Str("Martin")),
				 VList()), 
		  VTuple(VList(Int(2)),
				 VList(Str("Martin")),
				 VList(Str("Guido")),
				 VList(Str("Guido"))))
	  0 === 0
	}
  }
}
