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
import scala.collection.immutable
import anorm.SQL
import com.archerial._
import com.archerial.utils._
import SeqUtil.groupTuples
import com.archerial.utils.implicits._
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._

case class SelectGen(tree:TableTree, colExps: List[ColExp], colExps4Row: List[ColExp], tableExps:List[TableExp], whereConds:List[QueryExp],whereList:List[TableExp], outerTableIdMap:Option[TableIdMap]){
  assert(! tableExps.isEmpty, "require nonEmpty")
  assert(tree.node == tableExps.head, "hoge root")
  def rootTable:TableExp = tree.node
  lazy val _tableIdMap = TableIdMap.genTableIdMap(tableExps,outerTableIdMap)
  //assert(_tableIdMap.root == rootTable,"_tableIdMap.root == rootTable")
  val table_alias = 
	for {t <- tableExps
		 alias <- t.getColsRefTarget :: t.getKeyAliases(_tableIdMap).toList
		 if t != alias} yield (t,alias);
  val alias_table = 
	for {t <- (tableExps  ++ whereList)
		 val alias  = t.getColsRefTarget
		 if t != alias} yield (alias,t);
  lazy val tableIdMap, tableIdMapWithAlias = 
	(table_alias ++ alias_table).foldLeft(_tableIdMap){
	  case (map,(t,alias)) => map.addKeyAlias(t,alias)}
  //assert(tableIdMap.root == rootTable, "hoge root")

  def getConsts(exps:Seq[QueryExp]):Seq[ConstantQueryExp] =
	  exps.filter(_.isInstanceOf[ConstantQueryExp]).map(
		_.asInstanceOf[ConstantQueryExp])

  def getSQL(row:Option[Row]):(String,Seq[(String,RawVal)])= {
	val opWhereCondss = 
	  tableExps.map(_.getOptionalWhereConds(tableIdMap,row))
	val _whereConds = whereConds ++ opWhereCondss.flatten
	val ps = getConsts(QueryExpTools.getParameterExps(Left(_whereConds)))
	val idMap:TableIdMap = tableIdMap.addConstId(ps)
	val idMapWithAlias:TableIdMap = tableIdMapWithAlias.addConstId(ps)
	val tableClauses = tableExps.map{_ getSQL(idMap, None)}
	val colClauses = colExps.map{_ getSQL(idMap)}
	val wcs = _whereConds.map{_.getSQL(idMapWithAlias, None)} 
	val cs = colClauses.joinWith(", ")
	val ts = tableClauses.joinWith(" ")
	val ws = if (wcs.isEmpty) "" 
			 else " where %s" format(wcs.joinWith(" and "))
	
	val sql = "select %s from %s%s;" format(cs,ts,ws)
	(sql,ps.map((x) => idMap.constMap(x) -> x.rawVal))
  }

  def getRows(rows:Seq[Row])(implicit con: java.sql.Connection):Seq[Row] = {
	val rows2 = rootTable.filterRows(rows)
   	val newRows = 
	  for {row <- rows2
		   val (sql,ps) = getSQL(Some(row))
		   dataStream  <- getdata(sql,ps)}
   	  yield Row.gen(colExps4Row,dataStream)
	newRows
   }

  def getdata(sql:String,ps:Seq[(String,RawVal)])(implicit con:java.sql.Connection):Stream[List[Any]] = {
	  for (row <- SQL(sql).on(
		ps.map{case (s,r) =>
		  (s, r.toParameterValue)} :_*)())
	  yield row.data
  }

}

object SelectGen {

  def gen(tree:TableTree,colExps: Seq[ColExp], outerTableIdMap:Option[TableIdMap]=None) = {
	assert(colExps.nonEmpty,
		 "SelectGen.gen.colExps must be nonEmpty")
	val tableExps:Seq[TableExp] = tree.tableExps
	val tableExpsTail = tableExps.tail
	val rootTableExp = tableExps.head
	assert(rootTableExp == tree.node, "hoge root3")
	val wheresOrOthers = 
	groupTuples(for (x<- tableExpsTail) yield (x.isInstanceOf[WhereNode],x))
	val whereList = wheresOrOthers.getOrElse(true,Nil).map(_.asInstanceOf[WhereNode]).toList
	val tableList = rootTableExp :: wheresOrOthers.getOrElse(false,Nil).toList
	assert(! tableList.isEmpty, "require nonEmpty")
	val optCols = tree.getOptionalCols
	val normalCols = 
	  (colExps
	   ++ colExps.flatMap(_.tables.map(_.pk))
	 ).distinct.toList
	SelectGen(tree, 
		   (normalCols ++ optCols.map(_._2)),
		   (normalCols ++ optCols.map(_._1)),
		   tableList,whereList.map(_.cond),whereList,outerTableIdMap) 
  }
	
}