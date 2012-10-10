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

trait AbstractValueExp4Tree { 
  this:ValueExp => ;

  lazy val tableNodeList :List[TableExp] =
	ValueExpTools.getTableExps(this)

  lazy val table2children:TreeRel[TableExp] = 
	Rel.gen(tableNodeList)(
	  _.dependentParents).inverse.asTree(rootTable)

  lazy val rootTable:TableExp = 
	tableNodeList.headOption.getOrElse(UnitTable)

  lazy val col2table:Rel[ColNode,TableExp] = 
	Rel.gen(colList)((x:ColNode) => List(x.table))

  lazy val col2tableOM:Rel[ColNode,TableExp] = 
	Rel.gen(colListOM)((x:ColNode) => List(x.table))

  lazy val colList :List[ColNode] = 
	ValueExpTools.colNodeList(this).distinct.toList

  lazy val colListOM :List[ColNode] = 
	ValueExpTools.colNodeListOM(this).distinct.toList

}

trait AbstractValueExp extends AbstractValueExp4Tree{
  this:ValueExp => ;
  def eval(sp:ValueExp => List[TableTree]=SimpleGenTrees.gen)(implicit connection: java.sql.Connection):Seq[Value] = {
	val trees = sp(this)
	val colInfo = TreeColInfo(col2table,trees)
	val getter = RowsGetter(colInfo)(connection)
	val vs = eval(
	  UnitTable.pk,
	  List(UnitValue),
	  getter)
	vs
  }
  def rows2value(rows:Seq[Row] ): Seq[Value]
  def getSQL(map: TableIdMap):String
  def toShortString:String = toString

  def eval(colExp:ColExp, values:Seq[Value], getter:RowsGetter ): Seq[Value]
  def evalCol(colExp:ColExp):ColExp = colExp
  
  def getDependentCol():Stream[ColExp]

}