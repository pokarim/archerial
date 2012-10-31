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
import scala.collection.immutable.Set
import com.archerial.utils.implicits._
import com.archerial.utils._
import scalaz.{Value => _, _}, Scalaz._
import com.archerial._
import SeqUtil.groupTuples
import StateUtil.forS
import TableTree.{Tbx,TbxSet, TTree,TTrees}

import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._

case class TableTree(node:TableExp, children:List[TableTree]){
  def getAllTableExps = tableExps
   lazy val allTableExps = tableExps.toSet//getAllTableExps.toSet
   lazy val allTableExpsWithUnit = 
	 tableExps.toSet ++ depTables + UnitTable
  def contains(node:TableExp) = allTableExps.contains(node)

  lazy val tableExps :Stream[TableExp] = {
	val xs = node #:: children.flatMap(_.tableExps).toStream
	xs
  }

//	val tableExps = tree.getAllTableExps.toSeq
  val depTables = QueryExpTools.getContainTables(Right(tableExps)).filter(UnitTable != _)
  def getColExps(colInfo:TreeColInfo) = {
	(
	  tableExps.filter(!_.isGrouped).map(_.pk) ++ 
	  tableExps.filter(!_.isGrouped).flatMap(_.rootCol.toList) ++
	  depTables.flatMap(colInfo.table2col(_)) 
	).filter(_ != UnitTable.pk).distinct
  }

}


object TableTree {
  type Tbx = TableExp
  type TTree = TableTree
  type TTrees = Seq[TableTree]
  type TbxSet = Set[Tbx]
  def genTableTree(root:TableExp, map:Rel[TableExp,TableExp]):TableTree =
	TableTree(
	  root,
	  map(root).map(genTableTree(_,map)).toList
	)

}


