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
import anorm.SQL
import implicits._

trait Metadata extends {
  def all:List[Table]
  def create_all()(implicit connection: java.sql.Connection) = 
    for (table <- all)
	  SQL(table.createTableStr).execute()
}

object DDLUtil{
  def createTableStr(tablename:String, coldefs:List[String]):String =     "create table %s (%s);" format(
      tablename,  "\n  " + coldefs.reduceLeft(_ + ",\n  " + _ ))
}

case class ColType(str:String, isPrimaryKey:Boolean =false, isAutoInc: Boolean = false, isUniqueKey: Boolean =false){
  def getDef = 
	(List(str) ++ primaryDef ++ autoIncDef).joinWith(" ")
  def primaryDef = 
	if (isPrimaryKey) List("primary key") else Nil
  def autoIncDef = 
	if (isAutoInc) List("auto_increment") else Nil
  def primaryKey = copy(isPrimaryKey=true)
  def autoIncrement = copy(isAutoInc=true)
  def uniqueKey = copy(isUniqueKey=true)
  def isUnique = isUniqueKey || isPrimaryKey
}
object ColType{
  def int() = ColType("int")
  def varchar(x:Int) = ColType("varchar(%d)" format(x))
}

case class Column(val name:String, val coltype:ColType){
  def isPrimaryKey = coltype.isPrimaryKey
  def isUnique = coltype.isUnique
  override def toString:String = "Column(%s)" format(name)
  def getDef = name + " " + coltype.getDef
  
}

object ColumnName{
  def unapply(c:Column) = Some((c.name,c.name))

}
case class Table(val name:String, var columns:List[Column])
{
  def pk = primaryKey
  def primaryKey:Column = columns.filter(_.isPrimaryKey)(0)
  override def toString:String = "Table(%s)" format(name)

  def createTable()(implicit connection: java.sql.Connection) = 
	  SQL(createTableStr).execute()

  def createTableStr = DDLUtil.createTableStr(name, columns.map(_.getDef))
  def insertSQL(xs: List[(String,RawVal)]): anorm.SimpleSql[anorm.Row] = {
    val s = """insert into %s (%s) values (%s)  ;""" format(
      name,
      xs.map(_._1).joinWith(","),
	  xs.map(_._1).map("{%s}" format(_)).joinWith(","))
    SQL(s).on(xs.map{case (x,y) =>(x, y.toParameterValue)} :_*)
  }
  def insertRows(xss: List[(String,RawVal)]*)(implicit connection: java.sql.Connection) =
	xss.foreach(insertSQL(_).execute())
	
  def apply(colname:String):Column =
	columns.find(_.name == colname).get
}
