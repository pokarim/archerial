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

	val boss = Syain.boss
	val sub = ~boss
	val name = Syain.name
	val bossname = boss >>> name
	val subname = sub >>> name
	val syainId = Syain.Id.id
	val isMikio = name =:= Const(Str("mikio"))
	val isHokari = name =:= Const(Str("hokari"))
	
	"simple arrow" in {
	  name.valueExp.eval().toSet ===
		Set[Value](Str("hokari"),
				   Str("keiko"),
				   Str("mikio"))
	}
	"simple tuple" in {
	  Tuple(syainId, name).valueExp.eval().toSet === 
		Set(
		  VTuple(VList(Int(1)),
				 VList(Str("hokari"))),
		  VTuple(VList(Int(3)),
				 VList(Str("keiko"))),
		  VTuple(VList(Int(2)),
				 VList(Str("mikio"))))
	}
	"simple filter tuple" in {
	
	  (Filter(isMikio) >>> Tuple(
		syainId
		,name)).valueExp.eval().toSet === 
		  Set(
			VTuple(VList(Int(2)),
				   VList(Str("mikio"))))
	}
	"filter arrow" in {
	  ( (Filter(isHokari) >>> 
		 Tuple(syainId ,name,subname)
	   ).valueExp.eval().toSet) === 
	  Set(
	   VTuple(VList(Int(1)),
			  VList(Str("hokari")),
			  VList(Str("hokari"),Str("mikio"))
			))
	}

	"filter bossname subname" in {
	  ((Filter(isMikio) >>> Tuple(
		syainId
		,name
		,bossname
		,subname
	  )).valueExp.eval().toSet)
	  Tuple(
		syainId
		,name
		,boss >>> name
		,boss >>> Filter(boss >>> name =:= name) >>> name
	  ).valueExp.eval().toSet === Set(
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
	}
  }
}
