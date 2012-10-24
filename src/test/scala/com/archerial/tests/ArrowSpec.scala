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
	SampleData.insertSampleData

	val boss = Staff.boss
	val sub = ~boss
	val staffs = AllOf(Staff.Id)
	val name = Staff.name
	val bossname = boss >>> name
	val subname = sub >>> name
	val staffId = Staff.Id.id
	val isMikio = name =:= Const(Str("mikio"))
	val isHokari = name =:= Const(Str("hokari"))
if(false){	
	"simple arrow" in {
	  (staffs >>> name).queryExp.eval().toSet ===
		Set[Value](Str("hokari"),
				   Str("keiko"),
				   Str("mikio"))
	}
	"simple tuple" in {
	  (staffs >>> Tuple(staffId, name)).queryExp.eval().toSet === 
		Set(
		  VTuple(VList(Int(1)),
				 VList(Str("hokari"))),
		  VTuple(VList(Int(3)),
				 VList(Str("keiko"))),
		  VTuple(VList(Int(2)),
				 VList(Str("mikio"))))
	}
	"simple filter tuple" in {
	
	  (staffs >>> Filter(isMikio) >>> Tuple(
		staffId
		,name)).queryExp.eval().toSet === 
		  Set(
			VTuple(VList(Int(2)),
				   VList(Str("mikio"))))
	}
	"filter arrow" in {
	  ((staffs >>> Filter(isHokari) >>> 
		 Tuple(staffId ,name,subname)
	   ).queryExp.eval().toSet) === 
	  Set(
	   VTuple(VList(Int(1)),
			  VList(Str("hokari")),
			  VList(Str("hokari"),Str("mikio"))
			))
	}
  }
	"filter bossname subname" in {
	  // ((staffs >>> Filter(isMikio) >>> Tuple(
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
				 VList(Str("hokari")),
				 VList(Str("hokari")),
				 VList(Str("hokari"))), 
		  VTuple(VList(Int(3)),
				 VList(Str("keiko")),
				 VList(Str("mikio")),
				 VList()), 
		  VTuple(VList(Int(2)),
				 VList(Str("mikio")),
				 VList(Str("hokari")),
				 VList(Str("hokari"))))
	  0 === 0
	}
  }
}
