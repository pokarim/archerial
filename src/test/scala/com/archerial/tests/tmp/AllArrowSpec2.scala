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

class AllArrowSpec2 extends Specification {

  "The 'Hello world' string" should {
	val h2driver = Class.forName("org.h2.Driver")
	implicit val con = DriverManager.getConnection("jdbc:h2:mem:allarrowspec2", "", "")
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
	if (true){
	val a = staffs >>> //Filter(name =:= Const(Str("hokari"))) >>>
	  //Filter(boss >>> name  =:= Const(Str("mikio")))
	  //Filter(Any(sub >>> name  =:= Const(Str("mikio")))) //>>>
	  Filter(Any(sub >>> name  =:= Const(Str("keiko"))))>>>
	  Tuple(
	  staffId
	  ,name
		,sub >>> name
	,Filter(Any(sub >>> name  =:= Const(Str("keiko")))) //>>>
	//,Filter(boss >>> name =:= Const(Str("mikio")))
	  )
	val exp = a.queryExp
	  if(false){
		val trees = SimpleGenTrees.gen(exp)
		val tree= trees.head
		pprn("trees:",trees)
		val colInfo = TreeColInfo(
		  exp.col2tableOM,
		  trees)
		val select = SelectGen.gen(tree,colInfo.tree_col(tree))
		val (sql, ps) =select.getSQL(None)
		pprn(sql)
		pprn(exp.col2tableOM.pairs)
		pprn(("eval:",exp.eval()))
		//pprn(exp.eval())
	  }
	}	
	"start with 'Hello'" in {
      "Hello world" must startWith("Hello")
	}
	"end with 'world'" in {
      "Hello world" must endWith("world")
	}
  }
}
