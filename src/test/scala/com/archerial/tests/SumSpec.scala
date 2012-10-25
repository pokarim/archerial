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
//      "jdbc:mysql://localhost/db1?user=usr1&password=urpwd");
//com.mysql.jdbc.Driver

class SumSpec extends Specification {

  "The 'Hello world' string" should {
	val h2driver = Class.forName("org.h2.Driver")
	val mysqldriver = Class.forName("com.mysql.jdbc.Driver")
	implicit val con = 
	  DriverManager.getConnection("jdbc:h2:mem:sumspec", "", "")
	// implicit val con = 
	//   DriverManager.getConnection("jdbc:mysql://localhost/test02?user=hokari&password=")
	SampleData.tables.drop_all()
	SampleData.createTables 
	SampleData.insertSampleData
	val boss = Staff.boss
	val sub = ~boss
	val staff = AllOf(Staff.Id)
	val name = Staff.name
	val supname = boss >>> name
	val subname = sub >>> name
	val staffId = Staff.Id.id
	val isMikio = name =:= Const(Str("mikio"))
	val isHokari = name =:= Const(Str("hokari"))
	val onlyMikio = Filter(isMikio)
	val onlyHokari = Filter(isHokari)
	"any directly" in {
	  val sum1 = {
		AllOf(Order.Id) >>> Filter(Order.Id.id =:= Const(Int(1)))>>>
		Tuple(
		  Order.Id.id,
		  Order.orderItems >>> OrderItem.product >>> Product.name)}

	  val sum2 = {
		AllOf(Staff.Id) >>>
		Tuple(
		  Staff.Id.id,
		  ~Order.staff >>>
		  Order.orderItems >>> 
		  OrderItem.product >>> Product.name)}


	  // val exp2 = sum1.queryExp
	  // val exp = sum2.queryExp
val exp_ = 	  	  (staff >>> 
				    //Filter(staffId =:= Const(Int(2)))>>>
				   Tuple(
		staffId
		//,name
		//,boss >>> name
		,boss >>> Filter(boss >>> name =:= name) >>> name
	  )).queryExp





	  val complex1 = {
		AllOf(Order.Id) >>> Tuple(
		  Order.Id.id,
		  Sum(Order.orderItems >>> OrderItem.qty),
		  Sum(Order.staff >>> ~boss >>> Order.Id.id),
		  Sum(Order.orderItems >>> 
		  	  OrderItem.product >>>
		  	  Product.price),
		  Order.staff >>> Staff.name,
		  Order.orderItems >>> OrderItem.qty
		)
	  }
	  val _ = {
		Sum(AllOf(Order.Id) )
	  }
	  val arr = {
		AllOf(Order.Id) >>> Tuple(Order.Id.id,
								  Sum(Order.Id.id))
		
		AllOf(Staff.Id) >>> Tuple(
		  Staff.Id.id,
		  ~boss >>> Filter(name =:= Const(Str("keiko"))) >>>
		  name,
		  name)
		complex1
Sum(AllOf(Order.Id) )
		  // AllOf(Order.Id) >>> Tuple(Order.Id.id,
		  // 						  Sum(Order.Id.id))
	  }
	  //pprn(sum1.eval())
	  val exp = arr.queryExp
	  pprn(exp.eval(SimpleGenTrees.gen))
	  pprn(exp.eval(SimpleGenTrees.splitToPieces))
	  if(false){
		val trees = {
		  SimpleGenTrees.gen(exp)
		  SimpleGenTrees.splitToPieces(exp)
		}
		val tree= trees.head
		// pprn("tableNodeList",exp.tableNodeList)
		 pprn("trees:",trees)
//		 pprn("eval:",exp.eval())

		val colInfo = TreeColInfo(
		  exp.col2table, //OM
		  trees)
		val getter = RowsGetter(colInfo)
		//(getter.t2c2v2r)
		//pprn(colInfo.table2col.pairs)
		  val vs = exp.eval(
		    UnitTable.pk,
		    List(UnitValue),
		    getter)
		 pprn(vs)

	  }
	  0 === 0
	}
  }
}
