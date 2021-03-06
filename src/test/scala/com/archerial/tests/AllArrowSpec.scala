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
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._

class AllArrowSpec extends Specification {

  "The 'Hello world' string" should {
	val h2driver = Class.forName("org.h2.Driver")
	implicit val con = DriverManager.getConnection("jdbc:h2:mem:hallarrowspec", "", "")
	SampleData.createTables 
	SampleData.insertSampleData

	val boss = Staff.boss
	val sub = ~boss
	val staffs = AllOf(Staff.Id)
	val name = Staff.name
	val supname = boss >>> name
	val subname = sub >>> name
	val staffId = Staff.Id.id
	val isMikio = name =:= Const(Str("mikio"))
	val isHokari = name =:= Const(Str("hokari"))
	val onlyMikio = Filter(isMikio)
	val onlyHokari = Filter(isHokari)
	if(false){
	"any directly" in {
	  val a = {
		staffs >>> 
		Filter(Any(sub >>> name  =:= Const(Str("hokari")))) >>>
		Tuple(staffId,name)}.eval()
	  a.toSet === Set(
		VTuple(VList(Int(1)),
			   VList(Str("hokari"))))
	}
	"any tupled" in {
	  val b = {
	  staffs >>> 
		Filter(name  =:= Const(Str("hokari")))>>>
		Tuple(
		  staffId,
		  Filter(Any(sub >>> name  =:= Const(Str("hokari")))) >>> 		sub >>> name
		)}
	  b.eval().toSet === Set(
		VTuple(VList(Int(1)),
			   VList(Str("hokari"),Str("mikio"))
			 ))
	}
	"any tupled 2" in {
	  val b = {
	  staffs >>> 
		Filter(Any(NonNull(sub >>> sub >>> name)))  >>>
		Tuple(
		  staffId
		)}
	  b.eval().toSet === Set(
		VTuple(VList(Int(1))
			 ))
	}
  }
	"any tupled 3" in {
	  val b3 = {
	  staffs >>> Tuple(
		staffId
		//,Filter(NonNull(sub >>> sub >>> name))
		,NonNull(sub)
		//,Any(NonNull(sub >>> sub >>> name))
		)
	  }
	  pprn(b3.eval().toSet )
	  Set(
		VTuple(VList(Int(1)),
			   VList(Int(1)),
			   VList(Bool(true)),
			   VList(Bool(true))
			 ),
		VTuple(VList(Int(2)),
			   VList(),
			   VList(Bool(false)),
			   VList(Bool(false))
			 ),
		VTuple(VList(Int(3)),
			   VList(),
			   VList(Bool(false)),
			   VList(Bool(false))
			 )
	  )

	  val exp = b3.queryExp
	  if(true){
		//val trees = SimpleGenTrees.gen(exp)
		val trees = SimpleGenTrees.gen(exp)
		SimpleGenTrees.splitToPieces(exp)
		val tree= trees.head
		pprn("trees:",trees)

		val colInfo = TreeColInfo(
		  exp.col2table, //OM
		  trees)
		val getter = RowsGetter(colInfo,exp)
		pprn(exp)
		//pprn(exp.tableNodeList)
		
		val vs = exp.eval(
	  	  UnitTable.pk,
	  	  VList(UnitValue),
	  	  getter)
		 pprn(vs)
		// exp.eval()
	  }
	  0 === 0
	}
  }
}
