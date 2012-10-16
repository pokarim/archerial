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
	implicit val con = DriverManager.getConnection("jdbc:h2:mem:harrowspec2", "", "")
	SampleData.createTables 
	SampleData.insertSampleData

	val boss = Syain.boss
	val sub = ~boss
	val syains = AllOf(Syain.Id)
	val name = Syain.name
	val supname = boss >>> name
	val subname = sub >>> name
	val syainId = Syain.Id.id
	val isMikio = name =:= Const(Str("mikio"))
	val isHokari = name =:= Const(Str("hokari"))
	val onlyMikio = Filter(isMikio)
	val onlyHokari = Filter(isHokari)
	// println(name.queryExp.eval().toSet)
	
	if (true){
	val a = syains >>> Filter(name =:= Const(Str("hokari"))) >>>
	  Filter(Any(sub >>> name  =:= Const(Str("hokari")))) >>>Tuple(
	  syainId,
	  name
	  //sub >>> name
	  )

	  
	val exp = a.queryExp.asInstanceOf[NTuple]
	  pprn("exp", exp)
	  pprn(exp.tableNodeList)
	  // val any = exp.valExps.head.asInstanceOf[AnyQExp]
// 	  pprn("qexp", exp.tableNodeList)//table2children.pairs)
// 	  pprn("anyarrow",any)
// 	  val anyTrees  = SimpleGenTrees.gen(any)
// 	  pprn("anyTrees:",anyTrees)
// 	  val anyCondTrees  = SimpleGenTrees.gen(any.cond)
// 	  pprn("anyCondTrees:",anyCondTrees)
	  val trees = SimpleGenTrees.gen(exp)
// 	  val trees = SimpleGenTrees.gen(exp)//splitToPieces(exp)
 	  //pprn(trees)
	  val tree= trees.head
	  val colInfo = TreeColInfo(
		exp.col2table,
		trees)
	  //pprn(colInfo.table_tree.pairs)
	  
	  //exp.eval()


	  val select = SelectGen.gen(tree,colInfo.tree_col(tree))
	  pprn("tableIdMap",select.tableIdMap.map)
	  pprn(select.getSQL(None))
// 	//val newRows = select.getRows(comprows)
// pprn("eval:",exp.eval())
	  // val getter = RowsGetter(colInfo)
	  // pprn(getter.t2c2v2r)
	  // val vs = exp.eval(
	  // 	UnitTable.pk,
	  // 	List(UnitValue),
	  // 	getter)
	  // pprn(vs)
	}	
	
	"start with 'Hello'" in {
      "Hello world" must startWith("Hello")
	}
	"end with 'world'" in {
      "Hello world" must endWith("world")
	}
  }
}
