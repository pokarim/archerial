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

sealed trait Quantity
object Many extends Quantity
object One extends Quantity

object RowMulFactor extends Enumeration {
  val Many, One, LtOne = Value
}

sealed trait TableExp {
  def directParent:Option[TableExp]
  def dependentParents:List[TableExp] = 
	argCols.flatMap(_.tables)
  final def directAncestorsWithSelf:List[TableExp] = directAncestorsWithSelf_.reverse
  final def directAncestorsWithSelf_ :List[TableExp] = this :: directParent.map(_.directAncestorsWithSelf_ ).getOrElse(Nil)
  def argCols:List[ColExp]
  def rootCol:ColExp = argCols.head
  def filterRows(rows:Seq[Row]):Seq[Row] = rows
  final def isRoot(map:TableIdMap) = map.isRoot(this)
  def getSQL(map: TableIdMap):String = 
	if (isRoot(map))
	  "%s as %s" format(altRoot.name, map(this))
	  //altRoot.getSQLNonRoot(map.addKeyAlias(this,altRoot))
	else
	  getSQLNonRoot(map)
  
  def getSQLNonRoot(map: TableIdMap):String

  def getColsRefTarget:TableExp = this
  def pk = primaryKeyCol
  def primaryKeyCol:ColNode
  def altPrimaryKeyCol:ColNode = 
	ColNode(this, primaryKeyCol.column)
	// ColNode(altRoot, primaryKeyCol.column)
  def rowMulFactor:RowMulFactor.Value
  def getOptionalCols(isRoot:Boolean):List[(ColExp,ColExp)]
	

  final def getOptionalWhereConds(map: TableIdMap,row:Option[Row]):List[QueryExp] ={
	if (!isRoot(map)) Nil
	else optionalCondsWithRoot(map,row)
  }
	
  def optionalCondsWithRoot(map: TableIdMap,row:Option[Row]):List[QueryExp]

  def altRoot:Table
  def getKeyAliases(map: TableIdMap):Option[TableExp] = 
	None
	//if (isRoot(map)) Some(altRoot) else None
}

object UnitTable extends TableExp{
  def directParent:Option[TableExp] = None
  def argCols:List[ColExp] = List(primaryKeyCol)
  def getSQLNonRoot(map: TableIdMap):String =
	throw new Exception("not imple")
  def rowMulFactor:RowMulFactor.Value = RowMulFactor.One
  def getOptionalCols(isRoot:Boolean):List[(ColExp,ColExp)] = Nil
  def optionalCondsWithRoot(map: TableIdMap,row:Option[Row]):List[QueryExp] = Nil

  def altRoot:Table = throw new Exception("")
  val primaryKeyCol:ColNode = ColNode(UnitTable,
									  Column("UnitPK",ColType("UnitPK"))) 
  val UnitColExp = primaryKeyCol
}
import UnitTable.UnitColExp
case class TableNode(table: Table) extends TableExp{
  def directParent:Option[TableExp] = Some(UnitTable)
  def argCols:List[ColExp] = List(UnitTable.primaryKeyCol)

  def getOptionalCols(isRoot:Boolean):List[(ColExp,ColExp)] = Nil
  def optionalCondsWithRoot(map: TableIdMap,row:Option[Row]):List[QueryExp] = Nil
  
  def altRoot:Table = table
  def rowMulFactor:RowMulFactor.Value = RowMulFactor.One

  def primaryKeyCol:ColNode = ColNode(this,table.primaryKey)

  def getSQLNonRoot(map: TableIdMap):String = 
	"%s as %s" format(table.name, map(this))
}

case class WhereNode(tableNode: TableExp, cond :QueryExp) extends TableExp{
  def directParent:Option[TableExp] = Some(tableNode)
  def argCols:List[ColExp] = (tableNode.primaryKeyCol :: cond.getDependentCol.toList).distinct

  def primaryKeyCol:ColNode = ColNode(this,
									  tableNode.primaryKeyCol.column)

  def getOptionalCols(isRoot:Boolean):List[(ColExp,ColExp)] =
	if (isRoot)
	  List((tableNode.primaryKeyCol, altPrimaryKeyCol))
	else
	  List((tableNode.primaryKeyCol, altPrimaryKeyCol))

  def optionalCondsWithRoot(map: TableIdMap,row:Option[Row]):List[QueryExp]
  = {
	  List(OpExps.=:=(
		ConstantExp(row.get.d(tableNode.primaryKeyCol) match {
					  case Val(rawval,_) => rawval}), 
		Col(altPrimaryKeyCol)))}

  override def filterRows(rows:Seq[Row]):Seq[Row] = 
	rows.filter{(row)=>{
	  cond.row2value(row) match{
		  case Val(RawVal.Bool(true),_) 
		if row.d(tableNode.primaryKeyCol).nonNull => true
		  case _ => false }}}

  def altRoot:Table = getColsRefTarget.altRoot

  def rowMulFactor:RowMulFactor.Value = RowMulFactor.LtOne

  override def getColsRefTarget:TableExp = tableNode.getColsRefTarget
  def getSQLNonRoot(map: TableIdMap):String = {
	  throw new Exception("hogehoge")
  }
}

case class JoinNode(right:Table, leftcol:Col,rightcolumn:Column) extends TableExp{
  override def filterRows(rows:Seq[Row]):Seq[Row] = 
	rows.filter{_.d(leftcol.colNode).nonNull}

  def directParent:Option[TableExp] = Some(leftcol.table)
  def argCols:List[ColExp] = List(leftcol.colNode)
  def getOptionalCols(isRoot:Boolean):List[(ColExp,ColExp)] =
	if (isRoot)
	  List((leftcol.colNode, altLeftColNode))
	else
	  Nil
  def lefttable:TableExp = leftcol.colNode.table
  def rowMulFactor:RowMulFactor.Value = 
	if (rightcolumn.isUnique) RowMulFactor.One
	else RowMulFactor.Many
  def rightcol = ColNode(this, rightcolumn)

  def getSQLNonRoot(map: TableIdMap):String ={
	val l = leftcol.getSQL(map)
	val r = rightcol.getSQL(map)
	"left join %s as %s on %s = %s" format(
	  right.name, map(this), l,r)}

  def altRoot = right//TableNode(right)
  def primaryKeyCol:ColNode = ColNode(this,right.primaryKey)
  def altLeftColNode = ColNode(this,rightcolumn)
  //def altLeftColNode = ColNode(altRoot,rightcolumn)
  override def optionalCondsWithRoot(map: TableIdMap,row:Option[Row]):List[QueryExp] = 
	{
	  if (row.filter(_.contains(leftcol.colNode)).nonEmpty){
	  val Val(rawval,_) = row.get.d(leftcol.colNode)
	  List(OpExps.=:=(ConstantExp(rawval), 
					Col(altLeftColNode)
					))
	  }else{
	  List(OpExps.=:=(leftcol, 
					Col(altLeftColNode)
					))
		
	  }
	}
  def left:TableExp = leftcol.table

}

object JoinNode {
  def joinWith(rightTable:Table, leftCol:Col, rightColumn:Column):TableExp= 
	{
	  val doJoin = leftCol match {
		case Col(ColNode(tableNode, `rightColumn`)) => true
		case _ => false
	  }
	  leftCol match {
		case Col(ColNode(tableNode, `rightColumn`)) => tableNode
		case _ => JoinNode(rightTable, leftCol,rightColumn)
	  }
	}
}

