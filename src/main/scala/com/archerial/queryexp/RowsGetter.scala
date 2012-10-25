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

case class RowsGetter(colInfo:TreeColInfo)(implicit val con: java.sql.Connection){
  import RowTool.groupByCol
  def getmap() = {
	colInfo.trees.map(
	  (tree)=> tree2map(tree)).foldLeft(Map():T2C2V2R)(_ ++ _)
  }

  lazy val t2c2v2r:T2C2V2R = getmap()

  def tree2map(tree:TableTree) ={
	val ts = QueryExpTools.getContainTables(Right(tree.getAllTableExps.toSeq))
	val cols = ts.flatMap(
	  colInfo.table2col(_))
	val select = SelectGen.gen2(ts,cols,Nil)
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
	val rootPk = node.rootCol.tables.head.pk
	val anc = (node :: node.rootCol.tables).toSet
	val pk = node.pk
	val rows2:Seq[Row] = distinct(rows)((row)=>
	  for {(k,v) <- row.map
		   if k.tables.forall(node == _ ) || rootCol == k}
	  yield (k,v))
	val map2:C2V2R = map ++ 
	(for {col <- Set(rootCol,pk)}
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
