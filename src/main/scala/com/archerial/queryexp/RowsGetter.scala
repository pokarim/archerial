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

object `package` {
  type V2R = Map[Value,Seq[Row]]
  type C2V2R = Map[ColExp,V2R]
  type T2C2V2R = Map[TableExp,C2V2R]
  type Tx2R = Map[TableExp,Seq[Row]]
  type Tt2R = Map[TableTree,Seq[Row]]
}

object RelGetter{
  def pairs2cols(ps:Seq[(ColExp,ColExp)]):Seq[ColExp] =
	ps.flatMap{case (x,y)=> Seq(x,y)}.distinct


  def getPKs(x:ColExp,y:ColExp):Seq[ColExp] = {
	//t.allTableExpsWithUnit), p))
	val xt = x.tables.flatMap(QueryExpTools.getTableExps(_)).toSet
	val yt = y.tables.flatMap(QueryExpTools.getTableExps(_)).toSet
	val ts = (yt -- xt) ++ (x.tables ++ y.tables).toSet
	//ts.filter(!_.isGrouped).
	ts.map(_.pk).toSeq
  }
  def getDict(exp:QueryExp,trees:Seq[TableTree])(implicit con: java.sql.Connection):Map[(ColExp,ColExp),Map[Value,Seq[Value]]] = {
	val t2p = RelGetter.tree2pair(exp,trees)
	
	val dicts = for {tree <- trees}
	yield {
	  val isGrouped = !tree.tableExps.forall(!_.isGrouped)
	  val groupKeys = tree.tableExps.flatMap(_.groupKey).toSet

	  val ps = t2p(tree)
	  val ks = for {(x,y) <- ps;c <- getPKs(x,y)}yield c
	  
	  val cs = (ks ++pairs2cols(ps)).filter(_ != UnitTable.pk).distinct.filter((c) => 
		!isGrouped || 
        groupKeys(c) ||
		c.isAggregateFun)
	  if (cs.isEmpty) 
		Map[(ColExp,ColExp),Map[Value,Seq[Value]]]()
	  else{
	  val select = SelectGen.gen2(
		tree.depTables,cs
	   	,Nil)
	  val rows = select.getRows(List(Row()) )
	  val dict = (for {(x,y) <- ps} yield {
		val pks = getPKs(x,y)
		val rows2:Seq[Row] = distinct(rows){
		  (row)=>
			(for {pk <- List(x,y) ++ pks
				  if row.contains(pk) // TODO
				  val v = row.d(pk)} 
			 yield pk -> v).toSet}
		(x,y) -> groupTuples(for {r <- rows2} yield r.d(x) -> r.d(y))
	  }).toMap
	  dict
	  }
	}
	dicts.reduceLeft{(x,y) => x ++ y}
  }

  def tree2pair(qexp:QueryExp,trees:Seq[TableTree]):Map[TableTree,Seq[(ColExp,ColExp)]]={
	val pairs = QueryExpTools.getCols(qexp)
	val (ptail, map) = trees.foldLeft(
	  pairs.toSet -> 
	  Map[TableTree,Seq[(ColExp,ColExp)]]()){
	  case ((ps,map),t:TableTree)=> {
		val pss = groupTuples(
		  for {p@(x,y) <- ps.toSeq}
		  yield (
			{x.tables ++ y.tables}.toSet.subsetOf(
			  t.allTableExpsWithUnit), p))
		(pss.getOrElse(false,Nil).toSet, 
		 map ++ Map(t->pss.getOrElse(true,Nil)))
	  }}
	assert(ptail.isEmpty)
	map
  }
}
case class RelGetter(qexp:QueryExp,trees:Seq[TableTree]){
}

case class RowsGetter(colInfo:TreeColInfo,exp:QueryExp)(implicit val con: java.sql.Connection){

  val dict = RelGetter.getDict(exp,colInfo.trees)
  import RowTool.groupByCol
  def getmap() = {
	colInfo.trees.map(
	  (tree)=> tree2map(tree)).foldLeft(Map():T2C2V2R)(_ ++ _)
  }

  lazy val t2c2v2r:T2C2V2R = getmap()

  def tree2map(tree:TableTree) ={
	val cols = tree.getColExps(colInfo)
	val select = SelectGen.gen2(tree.depTables,cols.distinct,Nil)
	val rows = select.getRows(List(Row()) )
	val map = getMaps(tree,rows)
	map
  }
  def getMaps(tree:TableTree,rows:Seq[Row]):T2C2V2R ={
	val tables = UnitTable #:: tree.getAllTableExps.filter(
	  (t) => (!t.isGrouped || t.isInstanceOf[GroupByNode]))
	tables.foldLeft(Map(): T2C2V2R)(
	  (map : T2C2V2R, t : TableExp) =>
		getMapsC(tree, t,map,rows))
  }
  def getMapsC(tree:TableTree,node:TableExp,maps:T2C2V2R,rows:Seq[Row]):T2C2V2R = {
	val map:Map[ColExp,Map[Value,Seq[Row]]] = maps.getOrElse(node,Map[ColExp,Map[Value,Seq[Row]]]())
	val rootCol = node.rootCol
	val anc = (node :: node.rootCol.toList.flatMap(_.tables)).toSet
	val pk = node.pk
	val rows2:Seq[Row] = distinct(rows)((row)=>
	  for {(k,v) <- row.map
		   if k.tables.forall(node == _ ) || rootCol == Some(k)}
	  yield (k,v))
	val map2:C2V2R = map ++ 
	(for {col <- (pk::rootCol.toList ).toSet[ColExp]}
	yield col.normalize -> groupByCol(rows2,col)).toMap
	val maps3:T2C2V2R = maps ++ Map(node ->map2)
	maps3
  }

}
object RowTool {
  def groupByCol(rows:Seq[Row],col:ColExp):Map[Value,Seq[Row]] = {
	val root2row:Map[Value,Seq[Row]] = groupTuples({
	  for {row <- rows
		   val v = row.d(col)
		   if v.nonNull}
	  yield v -> Row(
		for {(k,v) <- row.map} yield  (k,v)
	  )}.distinct)
	root2row
  }
}
