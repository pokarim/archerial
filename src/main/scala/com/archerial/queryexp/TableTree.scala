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
  def argCols:List[ColExp] = (for {table <- tableExps
		 col <- table.argCols
		 if !col.tables.forall(contains(_))}
	yield col).toList

  def getOptionalCols() = 
	node.getOptionalCols(true) ++
  getAllTableExps.tail.flatMap(_.getOptionalCols(false))

  def getAllTableExps:Stream[TableExp] = node #:: children.toStream.flatMap(_.getAllTableExps)
  lazy val allTableExps = getAllTableExps.toSet
  def contains(node:TableExp) = allTableExps.contains(node)

  lazy val tableExps :Stream[TableExp] = {
	val xs = node #:: children.flatMap(_.tableExps).toStream
	xs
  }

  final def getForeignCols():Seq[ColExp] = 
  	for {table <- allTableExps.toSeq
		 colNode <- 
		 QueryExpTools.colNodeList(table)//TODO add COlQEXP
		 dp <- colNode.getTables
		 val colsParent = dp.getColsRefTarget
		 if !contains(colsParent)}
	yield colNode

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


