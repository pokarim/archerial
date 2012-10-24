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





	  val arr_ = {
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
	  }
	  //pprn(sum1.eval())
	  val exp = arr.queryExp
	  if(true){
		//val trees = SimpleGenTrees.gen(exp)
		val trees = SimpleGenTrees.gen(exp)
		SimpleGenTrees.splitToPieces(exp)
		val tree= trees.head
		pprn("tableNodeList",exp.tableNodeList)
		pprn("trees:",trees)
		// pprn("t2c",exp.table2children.pairs)
		// pprn("t2c.root",exp.table2children.root)
		val colInfo = TreeColInfo(
		  exp.col2table, //OM
		  trees)
		val getter = RowsGetter(colInfo)
		// pprn(exp)
		// pprn(exp.tableNodeList)
		import QueryExpTools._
		val ntuple = directParents(Left(exp))(1)
		val sum = directParents(ntuple)
		// pprn("colListOM",exp.colListOM)
		// pprn("colList",exp.colList)
		val select = SelectGen.gen(tree,colInfo.tree_col(tree))
		 val (sql, ps) =select.getSQL(None)
		 pprn(sql)

		pprn("eval:",exp.eval())

		// pprn(getter.t2c2v2r)
		// pprn(trees.head.getAllTableExps)
		// val vs = exp.eval(
	  	//   UnitTable.pk,
	  	//   List(UnitValue),
	  	//   getter)
		// pprn(vs)
		// exp.eval()


		//exp.eval()

		// val select = SelectGen.gen(tree,colInfo.tree_col(tree))
		// val (sql, ps) =select.getSQL(None)
		// pprn(sql)
		// pprn(exp.col2tableOM.pairs)
		// pprn(("eval:",exp.eval()))
		//pprn(exp.eval())
	  }
	  0 === 0
	}
  }
}
