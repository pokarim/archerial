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
import com.archerial._
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._
import scala.collection.immutable,immutable.Map
import com.archerial.utils._

import com.archerial.utils.implicits._

import SeqUtil.{groupTuples,distinct}

object RowsGetter{
  import QueryExpTools.{getTableExps,getCols}
  type CPair = (ColExp,ColExp)
  type CPairs = Seq[(ColExp,ColExp)]
  def getPKs(x:ColExp,y:ColExp):Set[ColExp] = {
	val xt = x.tables.flatMap(getTableExps).toSet
	val yt = y.tables.flatMap(getTableExps).toSet
	val ts = (yt -- xt) ++ x.tables ++ y.tables
	ts.map(_.pk) ++ List(x,y)
  }
  def getPair2Col(tree:TableTree,t2p:Map[TableTree,CPairs]):Seq[(CPair,Seq[ColExp])] = {
	val isGrouped = !tree.tableExps.forall(!_.isGrouped)
	val groupKeys = tree.tableExps.flatMap(_.groupKey).toSet
	val ps = t2p(tree)
	for {p@(x,y) <- ps}
	yield p -> 
	getPKs(x,y).filter(
	  (c) => c != UnitTable.pk && 
	  (!isGrouped || c.isAggregateFun ||groupKeys(c) )).toSeq
  }

  def getDictOfTree(tree:TableTree,t2p:Map[TableTree,CPairs])(implicit con: java.sql.Connection)={
	val p2c = getPair2Col(tree,t2p).toMap
	val allcols = p2c.values.flatten.toSeq.distinct
	lazy val select = SelectGen.gen2(tree.depTables,allcols,Nil)
	lazy val rows = select.getRows(List(Row()))
	for {(p@(x,y),cs) <- p2c} 
	yield p -> groupTuples(
	  distinct(rows)((r) => cs.map(r(_))).map{
		(r) => r(x) -> r(y)})
  }
  def getDict(exp:QueryExp,trees:Seq[TableTree])(implicit con: java.sql.Connection):Map[CPair,Map[Value,Seq[Value]]] = {
	val t2p = tree2pair(exp,trees)
	trees.map(getDictOfTree(_,t2p)).reduceLeft{_ ++ _}
  }

  def tree2pair(qexp:QueryExp,trees:Seq[TableTree]):Map[TableTree,CPairs]={
	val (ptail, map) = trees.foldLeft(
	  getCols(qexp).toSet -> 
	  Map[TableTree,Seq[(ColExp,ColExp)]]()){
	  case ((ps,map),t:TableTree)=> {
		val pss = groupTuples(
		  for {p@(x,y) <- ps.toSeq}
		  yield {
			pprn("x.tables:",x.tables)
			pprn("y.tables:",y.tables)
			pprn("all:",t.allTableExpsWithUnit)

			({x.tables ++ y.tables}.toSet.subsetOf(
			  t.allTableExpsWithUnit), p)})
		(pss.getOrElse(false,Nil).toSet, 
		 map ++ Map(t->pss.getOrElse(true,Nil)))
	  }}
	//assert(ptail.isEmpty)
	pprn("ptail",ptail)
	map
  }
}

case class RowsGetter(colInfo:TreeColInfo,exp:QueryExp)(implicit val con: java.sql.Connection){

  val dict = RowsGetter.getDict(exp,colInfo.trees)
}
