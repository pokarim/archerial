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

package com.archerial
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._
import scala.collection.immutable,immutable.Map

import implicits._

import SeqUtil.{groupTuples,distinct}

object `package` {
  type V2R = Map[Value,Seq[Row]]
  type C2V2R = Map[ColExp,V2R]
  type T2C2V2R = Map[TableExp,C2V2R]
  type Tx2R = Map[TableExp,Seq[Row]]
  type Tt2R = Map[TableTree,Seq[Row]]
}
case class RowsGetter(colInfo:TreeColInfo)(implicit val con: java.sql.Connection){
  val trees = colInfo.trees
  lazy val tree2rows_t2c2v2r = getTree2RowsAndT2c2v2r()
  lazy val tree2rows = tree2rows_t2c2v2r._1
  lazy val t2c2v2r = tree2rows_t2c2v2r._2
  lazy val c2v2r:C2V2R = for {(t,c2v2r) <- t2c2v2r
						(c,v2r) <- c2v2r
					  } yield (c,v2r)


  def getRowsMaps(mt :(Map[TableTree,Seq[Row]], T2C2V2R), tree:TableTree):(Map[TableTree,Seq[Row]],T2C2V2R) = {
	val (t2r, t2c2v2r) = mt
	val map = getTx2Rows(t2r, t2c2v2r, tree)
	val maps = t2c2v2r ++ getMaps(tree,map(tree))
	(map,maps)
  }
  def getTree2RowsAndT2c2v2r():(Tt2R,T2C2V2R) = {
	  trees.foldLeft((Map[TableTree,Seq[Row]](),Map():T2C2V2R))(
	  getRowsMaps)
  }

  def getTx2Rows(t2r:Map[TableTree,Seq[Row]],t2c2v2r:T2C2V2R, tree:TableTree):Map[TableTree,Seq[Row]] = {
	val newRows = getRows(t2c2v2r,tree)
	t2r ++ Map(tree -> newRows)
  }


  def getRows(t2c2v2r:T2C2V2R, tree:TableTree):Seq[Row]
  = {
	val argcol2table = Rel.gen(tree.argCols)(_.tables)
	val table2argcol = argcol2table.inverse
	val ancLists:Seq[List[TableExp]] = 
	  for {table <- argcol2table.rights}
	  yield table.directAncestorsWithSelf
	val (List(ttree), dropped) = Tree.dropTrunk(ancLists)
	val rootCol = ttree.node.rootCol
	val comprows = if(ttree.node == UnitTable)List(Row()) else {
	  val rootValues = t2c2v2r(ttree.node)(rootCol).keys.toSeq
	  rootValues.flatMap(
		getCompRows(_,ttree,table2argcol,t2c2v2r))
	}
	val select = Select.gen(tree,colInfo.tree_col(tree))
	val newRows = select.getRows(comprows)
	newRows
  }

  def getMaps(tree:TableTree,rows:Seq[Row]):T2C2V2R ={
	tree.getAllTableExps.foldLeft(Map(): T2C2V2R)(
	  (map : T2C2V2R, t : TableExp) =>
	  getMapsC(t,map,rows)
	  )
  }

  def getMapsC(node:TableExp,maps:T2C2V2R,rows:Seq[Row]):T2C2V2R = {
	val map:Map[ColExp,Map[Value,Seq[Row]]] = maps.getOrElse(node,Map[ColExp,Map[Value,Seq[Row]]]())
	val ancestorsOrSelf = ValueExpTools.getTableExps(node).toSet
	val rows2 = distinct(rows)((row)=>
	  for {(k,v) <- row.map
		   if k.tables.forall(ancestorsOrSelf(_))} yield (k,v)
							 )
	val root2row:Map[Value,Seq[Row]] = groupTuples({for {row <- rows2}
					  yield row.d(node.rootCol) -> Row(
						for {(k,v) <- row.map } yield  (k,v)
					  )}.distinct)
	val pk2row:Map[Value,Seq[Row]] = 
	  groupTuples({for {row <- rows2}
					  yield row.d(node.pk) -> Row(
						for {(k,v) <- row.map } yield  (k,v)
					  )}.distinct)
	val map2:C2V2R = map ++ Map(
	  node.rootCol->root2row,
	   node.pk -> pk2row
	)
	val maps3:T2C2V2R = maps ++ Map(node ->map2)
	maps3
  }

  def getCompRows(rootValue:Value,ttree:Tree[TableExp],table2argcol:Rel[TableExp,ColExp],t2c2v2r:T2C2V2R):Seq[Row] = {
	val node = ttree.node
	assert(node != UnitTable, "is not UnitTable")
	val pks = t2c2v2r(node)(node.rootCol)(rootValue).map(_.d(node.pk))
	val rows = for {pk <- pks
					row <- t2c2v2r(node)(node.pk)(pk)}
			   yield row

	val argcols = table2argcol(node)
	val rows2 = 
	  for {row <- rows}
	  yield {
		val rowss:Seq[Seq[Row]] = 
		  for {ctree <- ttree.children} 
		  yield getCompRows(
			row.d(ctree.node.rootCol)
			,ctree,table2argcol,t2c2v2r)
		val rowHere = Row(argcols.map((x) => x->row.d(x)))
		rowss.foldLeft(List(rowHere))((rowsL,rowsR) => 
		  for {l <- rowsL
			   r <- rowsR} 
		  yield Row(l.map ++ r.map))
	  }
	rows2.flatten.distinct
  }
	
}
