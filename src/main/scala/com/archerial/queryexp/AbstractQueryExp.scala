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
import com.archerial.utils.{Rel,TreeRel}

import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._

trait AbstractQueryExp4Tree { 
  this:QueryExp => ;

  lazy val tableNodeList :List[TableExp] =
	QueryExpTools.getTableExps(this).filter(_ != UnitTable)

  lazy val table2children:TreeRel[TableExp] = 
	Rel.gen(tableNodeList)(_.dependentParents).inverse.asTree(rootTable)

  lazy val rootTable:TableExp = 
	tableNodeList.headOption.getOrElse(UnitTable)

  lazy val col2table:Rel[ColExp,TableExp] = 
	Rel.gen[ColExp,TableExp](colList)((x:ColExp) => x.tables)

  lazy val col2tableOM:Rel[ColExp,TableExp] = 
	Rel.gen[ColExp,TableExp](colListOM)((x:ColExp) => x.tables)

  lazy val colList :List[ColExp] = 
	QueryExpTools.colNodeList(this).distinct.toList

  lazy val colListOM :List[ColExp] = 
	QueryExpTools.colNodeListOM(this).distinct.toList

}

trait AbstractQueryExp extends AbstractQueryExp4Tree{
  this:QueryExp => ;
  def eval(sp:QueryExp => List[TableTree]=SimpleGenTrees.gen)(implicit connection: java.sql.Connection):SeqValue = {
	val trees = sp(this)
	val colInfo = TreeColInfo(col2table,trees)
	val getter = RowsGetter(colInfo,this)(connection)
	val vs = eval(
	  UnitTable.pk,
	  VList(Seq(UnitValue)),
	  getter)
	vs
  }
  def constants:Seq[ConstantQueryExp] = Nil

  def row2value(row:Row ): Value
  def getSQL(map: TableIdMap):String

  def toShortString:String = toString

  def eval(colExp:ColExp, values:SeqValue, getter:RowsGetter ): SeqValue
  def evalCol(colExp:ColExp):ColExp = colExp
  
  def getDependentCol():Stream[ColExp]

}
