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

sealed trait ColExp {
  def normalize = this
  def constants:Seq[ConstantQueryExp] = Nil
  def getSQL(map: TableIdMap):String
  def parse(x:Any):RawVal = x match {
	case null => RawVal.Null
	case x:Int => RawVal.Int(x)
	case x:String => RawVal.Str(x)
	case x:Boolean => RawVal.Bool(x)
  }
  def parse2Val(x:Any):Value = parse(x) match{
	case x: RawVal => Val(x,1)
  }
  def getColNodes:List[ColNode]
  def getTables:List[TableExp]
  def tables:List[TableExp] = getTables
  def columns:List[Column] = this match {
	case ColNode(_,c) => List(c)
	case _ => Nil
  }
  def isPrimaryKey:Boolean = false
}
case class ColNode(table: TableExp, column: Column) extends ColExp{
  override def isPrimaryKey:Boolean = column.isPrimaryKey
  def getTables:List[TableExp] = List(table)
  def getColNodes:List[ColNode] = List(this)
  def getSQL(map: TableIdMap):String = {
	"%s.%s" format(map(table), column.name)
  }
  override def normalize = copy(table=table.getColsRefTarget)

}

case class ConstantColExp(table: TableExp, value: RawVal) extends ColExp {
  def getTables:List[TableExp] = List(table)
  def getColNodes:List[ColNode] = Nil
  def getSQL(map: TableIdMap):String = 
	value.toSQLString
}

case class QExpCol(table:TableExp,exp:QueryExp) extends ColExp{
  override def constants:Seq[ConstantQueryExp] = exp.constants
  def getTables:List[TableExp] = List(table)
  def getColNodes:List[ColNode] = Nil
  def getSQL(map: TableIdMap):String = exp.getSQL(map)
}
