/**
 * Copyright 2012 Martin Hokari
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

class ArrowSpec2 extends Specification {

  "The 'Hello world' string" should {
	val h2driver = Class.forName("org.h2.Driver")
	implicit val con = DriverManager.getConnection("jdbc:h2:mem:harrowspec2", "", "")
	SampleData.createTables 

	//SampleData.insertSampleData
	if(true)	{
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
	val supname = boss >>> name
	val subname = sub >>> name
	val staffId = Staff.Id.id
	val isMartin = name =:= Const(Str("Martin"))
	val isGuido = name =:= Const(Str("Guido"))
	val onlyMartin = Filter(isMartin)
	val onlyGuido = Filter(isGuido)
	// println(name.queryExp.eval().toSet)
	
	if (true)
	(staffs >>> name).queryExp.eval().toSet ===
	  Set[Value](Str("Guido"),
				 Str("Larry"),
				 Str("Martin"))
	//pprn(staffs.eval())
	//pprn(ConstantArrow(Int(1)).eval())
	//pprn(
	if (false){
	val a = staffs >>> Tuple(
	  staffId,
	  Const(Int(8)),
	  sub >>> Const(Int(7)),
	  sub )

	  
	val exp = a.queryExp
	  pprn(exp)
	  pprn(exp.tableNodeList)
	  //val trees = SimpleGenTrees.splitToPieces(exp)
	  val trees = SimpleGenTrees.gen(exp)//splitToPieces(exp)
	  pprn(trees)
	  val colInfo = TreeColInfo(exp.col2table,trees)
	  val getter = RowsGetter(colInfo,exp)
	  //pprn("table_tree:",colInfo.table_tree.pairs)
	  val vs = exp.eval(
	  	UnitTable.pk,
	  	List(UnitValue),
	  	getter)
	  pprn(vs)
	}	
	 //).eval())
	//pprn((staffs >>> name ).eval())
	if (true)
	{staffs >>> Tuple(staffId, name)}.queryExp.eval().toSet === 
	  Set(
		VTuple(VList(Int(1)),
			   VList(Str("Guido"))),
		VTuple(VList(Int(3)),
			   VList(Str("Larry"))),
		VTuple(VList(Int(2)),
			   VList(Str("Martin"))))
	
	if (true)
	(staffs >>> Filter(isMartin) >>> Tuple(
	  staffId
	  ,name)).queryExp.eval().toSet === 
		Set(
		  VTuple(VList(Int(2)),
				 VList(Str("Martin"))))
	if(false)
	{
	  val arr = AllOf(Staff.Id) >>> Filter(isGuido) >>> 
		Tuple(staffId ,name,subname)
	  val exp = arr.queryExp
	  //val trees = SimpleGenTrees.gen(exp)
	  val trees = SimpleGenTrees.splitToPieces(exp)

	  pprn("AA")
	  pprn(exp)
	  pprn("HOGE")
	  pprn(trees)
	  pprn("FUGa")
	  
	  val colInfo = TreeColInfo(exp.col2table,trees)
	  val getter = RowsGetter(colInfo,exp)
	  //pprn("table_tree:",colInfo.table_tree.pairs)
	  val vs = exp.eval(
	  	UnitTable.pk,
	  	List(UnitValue),
	  	getter)

	  val join = trees(2).node
	  // pprn("where",where)
	  // pprn("where",QueryExpTools.getTableExps(where))
	  pprn(vs)
	  Set(
	   VTuple(VList(Int(1)),
			  VList(Str("Guido")),
			  VList(Str("Martin"),Str("Larry"))
			))
	  pprn()
	}
	if (false)
	(	(Filter(isMartin) >>> Tuple(
	  staffId
	  ,name
	  ,supname
	  ,subname
	)).queryExp.eval().toSet)

	if (true)
	(staffs >>> Tuple(
	  staffId
	  ,name
	  ,boss >>> name
	  ,boss >>> Filter(boss >>> name =:= name) >>> name
	)).queryExp.eval().toSet === Set(
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

	val arr1 = Tuple(
	  staffId
	  ,name
	  ,boss >>> name
	  ,boss >>> Filter(boss >>> name =:= name) >>> name
	)
	
	
	val arrows = List(
	  staffs >>> Tuple(
		staffId
		,name
		,boss >>> name
		,boss >>> Filter(boss >>> name =:= name) >>> name
	  )
	  , staffs >>> onlyMartin >>> name
	  ,staffs >>> boss >>> boss >>> onlyMartin >>> onlyMartin >>> onlyMartin >>> name
	  ,staffs >>> Filter(isGuido) >>> 
	  	Tuple(staffId ,name,subname)
	  // ,staffs >>> Tuple(
	  // 	staffId,
	  // 	ConstantArrow(Int(8)),
	  // 	sub >>> ConstantArrow(Int(9)))
	  
	  
	)
	import RawValImplicits._
	val values = List[List[Value]](
	  List(
		VTuple(
		  1.one,"Guido".one,"Guido".one,"Guido".one), 
		VTuple(
		  2.one,"Martin".one, "Guido".one, "Guido".one), 
		VTuple(
		  3.one,"Larry".one, "Martin".one, VList())
	  )
	  ,List[Value]("Martin")
	  ,List[Value]()
	  ,List[Value](VTuple(
		1.one,"Guido".one,VList("Guido","Martin")
	  ))
	  ,List[Value](
		VTuple(
		1.one,8.one,VList(9,9)),
		VTuple(
		3.one,8.one,VList()),
		VTuple(
		2.one,8.one,VList(9))
	  ))
	
	val splitters = List[QueryExp => List[TableTree]](
	  SimpleGenTrees.gen, SimpleGenTrees.splitToPieces)
	
	// pprn(arrows(0).eval())



	if(false){
	  val arr = Tuple(
		staffId
		,name
		,boss >>> name
		//,boss >>> FilterArrow(boss >>> name =:= name) >>> name
	  )
	  val exp = arr.queryExp
	  val trees = SimpleGenTrees.gen(exp)
	  val colInfo = TreeColInfo(exp.col2table,trees)
	  val getter = RowsGetter(colInfo,exp)
	  val vs = exp.eval(
	  	UnitTable.pk,
	  	List(UnitValue),
	  	getter)
	}
	if(true)
	  "contain 11 characters" in {
	  dopprn.withValue(true){
		for {(a,v) <- arrows.zip(values).slice(3,4)}{
		  println("*****************(((((((((((((((**********")
		  val cexp = a.queryExp
		  pprn(a)
		  pprn(cexp)
		  // pprn("cexp:",cexp)
		  val values = splitters.slice(0,100).map((s)=>a.queryExp.eval(s))
		  if(!values.forall(_.toSet == v.toSet)){
			pprn("values:",values.map(_.toSet))
			pprn("v",v.toSet)
		  }
		  //values.map(_.toSet === v.toSet)
		   0 === 0
		}
	  }
	  }
	
	if (false)
	pprn(arr1.queryExp.eval().toSet)
	"contain 11 characters" in {
	  "Hello world".size === 11
	  //"Hello world" must have size(11)
	}
	
	"start with 'Hello'" in {
      "Hello world" must startWith("Hello")
	}
	"end with 'world'" in {
      "Hello world" must endWith("world")
	}
  }
}
