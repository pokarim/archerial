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
package com.archerial.queryexp

import com.archerial.Value
import com.archerial.utils._
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._
import scala.collection.immutable

object ColEvalTool{
  def eval(colExp:ColExp, table:TableExp, vcol:ColExp, values:Seq[Value], getter:RowsGetter ): Seq[Value] = {
	val tree = getter.colInfo.table_tree.one(table)
	val vtrees = getter.colInfo.table_tree(vcol.tables.head)
	val (ptree,pcol,pvalues) =
	if (!vtrees.contains(tree) ){
	  val ptree = 
		getter.colInfo.table_tree.one(
		  tree.node.directParent.get)
	  val pcol = tree.node.rootCol.asInstanceOf[ColNode]//TODO
	  val ptree2 = getter.colInfo.table_tree.one(pcol.tables.head)
	  assert(ptree == ptree2,"ptree == ptree2")
	  val pvals = Col(pcol).eval(vcol,values,getter)
	  val pcol2 = Col(pcol).evalCol(pcol)
	  (ptree,pcol2,pvals)
	}else{
	  (tree,vcol,values)
	}
	val c2v2r = 
	  if (getter.t2c2v2r(table).contains(pcol) 
		  || pcol == UnitTable.pk)
		getter.t2c2v2r(table)
	  else
		getter.t2c2v2r(pcol.tables.head)
	
	if (pcol == UnitTable.pk){
	  if(colExp ==table.pk){
		c2v2r(colExp).keys.toSeq.filter(_.nonNull)
	  }else{
		c2v2r(table.pk).values.toSeq.flatMap(_.map(_.d(colExp))).filter(_.nonNull)
	  }
	} else {
	  for {pv <- pvalues;
		   row <- c2v2r(pcol).getOrElse(pv,Nil)
		   val v = row.d(colExp)
		   if v.nonNull}
	  yield v
	}
  }
}



trait TupleExpBase {
  def row2value(row:Row ): Value =
	 throw new Exception("hoge")

  def getSQL(map: TableIdMap):String = {
	throw new Exception("invalid operation")
  }

  def getDependentCol():Stream[ColExp] = 
	(keyExp::valExps).toStream.flatMap(_.getDependentCol())

  def keyExp:QueryExp
  def valExps:List[QueryExp]
}