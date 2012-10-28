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

case class SelectGen(rootTable:TableExp,/*tree:TableTree, */ colExps: List[ColExp], /* colExps4Row: List[ColExp], */ tableExps:List[TableExp], whereConds:List[QueryExp],whereList:List[TableExp], groupList:List[TableExp], outerTableIdMap:Option[TableIdMap]){
  assert(! tableExps.isEmpty, "require nonEmpty")
  //assert(tree.node == tableExps.head, "hoge root")
  //def rootTable:TableExp = tree.node
  lazy val _tableIdMap = TableIdMap.genTableIdMap(tableExps,outerTableIdMap)

  val table_alias:Seq[(TableExp,TableExp)] =// Nil
	for {t <- tableExps
		 alias <- t.getColsRefTarget :: t.getKeyAliases(_tableIdMap).toList
		 if t != alias} yield (t,alias);
  val alias_table:Seq[(TableExp,TableExp)] =// Nil
	for {t <- (tableExps  ++ whereList ++ groupList)
		 val alias  = t.getColsRefTarget
		 if t != alias} yield (alias,t);
  lazy val tableIdMap, tableIdMapWithAlias = 
	(table_alias ++ alias_table).foldLeft(_tableIdMap){
	  case (map,(t,alias)) 
	  =>
		if (t == UnitTable || 
			outerTableIdMap.nonEmpty &&
			outerTableIdMap.get.map.contains(alias)) map
		else 
		  map.addKeyAlias(t,alias)
	}

  def getSQL(row:Option[Row]):(String,Seq[(String,RawVal)])= {
	val opWhereCondss = 
	  tableExps.map(_.getOptionalWhereConds(tableIdMap,row))
	val _whereConds = whereConds ++ opWhereCondss.flatten
	val ps = QueryExpTools.getConsts(Left(_whereConds)) ++
	colExps.flatMap(_.constants)
	val idMap:TableIdMap = tableIdMap.addConstId(ps)
	val idMapWithAlias:TableIdMap = tableIdMapWithAlias.addConstId(ps)
	val tableClauses = tableExps.map{_ getSQL(idMap)}
	val colClauses = colExps.map{_ getSQL(idMap)}
	val wcs = _whereConds.map{_.getSQL(idMapWithAlias)} 
	val gls = groupList.map{_.getSQL(idMapWithAlias)} 
	val cs = colClauses.joinWith(", ")
	val ts = tableClauses.joinWith(" ")
	val ws = if (wcs.isEmpty) "" 
			 else " where %s" format(wcs.joinWith(" and "))
	val gs = 
	  if (gls.isEmpty) ""
	  else gls.head//.getSQL()
	  
	val sql = "select %s from %s%s%s" format(cs,ts,ws,gs)
	pprn(sql)
	(sql,ps.map((x) => idMap.constMap(x) -> x.rawVal))
  }

  def getRows(rows:Seq[Row])(implicit con: java.sql.Connection):Seq[Row] = {
	val rows2 = rootTable.filterRows(rows)
   	val newRows = 
	  for {row <- rows2
		   val (sql,ps) = getSQL(Some(row))
		   dataStream <- getdata(sql,ps)}
   	  yield Row.gen(colExps, dataStream)
   	  //yield Row.gen(colExps4Row, dataStream)
	newRows
   }

  def getdata(sql:String,ps:Seq[(String,RawVal)])(implicit con:java.sql.Connection):Stream[List[Any]] = {
	val values = ps.distinct.map{case (s, r) =>
		  (s, r.toParameterValue)}
	for (row <- SQL(sql+";").on(values:_*)())
	yield row.data
  }

}

object SelectGen {


  def gen2(tableExps:Seq[TableExp], colExps: Seq[ColExp], getOptionalCols:List[(ColExp,ColExp)], outerTableIdMap:Option[TableIdMap]=None) = {
	assert(colExps.nonEmpty,
		 "SelectGen.gen.colExps must be nonEmpty")
	//val tableExps:Seq[TableExp] = tableExps
	val tableExpsTail = tableExps.tail
	val rootTableExp = tableExps.head
	//assert(rootTableExp == tree.node, "hoge root3")
	val wheresOrOthers = 
	  groupTuples{for (x <- tableExpsTail) yield 
		x.getTableType -> x}
	  //(x.isInstanceOf[WhereNode],x))
	val whereList = wheresOrOthers.getOrElse(
	  TableNodeType.Where,Nil).map(_.asInstanceOf[WhereNode]).toList
	val tableList = rootTableExp :: wheresOrOthers.getOrElse(
	  TableNodeType.Table,Nil).toList
	val groupList = 
	  for {
		g@GroupByNode(table,key) <-
		wheresOrOthers.getOrElse(TableNodeType.GroupBy,Nil).toList
		if table != UnitTable}
	yield g//.asInstanceOf[GroupByNode]

//.map(_.asInstanceOf[GroupByNode]).toList
	assert(! tableList.isEmpty, "require nonEmpty")
	val optCols = getOptionalCols//tree.getOptionalCols
	val normalCols = 
	  (colExps
	   ++ colExps.flatMap(_.tables.filter(!_.isGrouped).map(_.pk))
	 ).distinct.toList
	SelectGen(rootTableExp, 
			  normalCols,
			  tableList,whereList.map(_.cond),whereList,groupList,outerTableIdMap) 
  }


  def gen(tree:TableTree,colExps: Seq[ColExp], outerTableIdMap:Option[TableIdMap]=None) = {
	val opts = {
	  //tree.getOptionalCols:List[(ColExp,ColExp)]
	  Nil
	}
gen2(tree.tableExps, colExps: Seq[ColExp], opts, outerTableIdMap:Option[TableIdMap])
  }
	
}
