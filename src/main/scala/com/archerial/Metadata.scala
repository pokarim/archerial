package com.archerial

import anorm.SQL


abstract class Metadata extends {
  implicit val implicitMetadata = this
  override def toString = "Metadata()"
  def all:List[Table]
  def create_all()(implicit connection: java.sql.Connection) = 
    for (table <- all)
	  SQL(table.createTable).execute()
}


object DDLUtil{
  def createTable(tablename:String, coldefs:List[String]):String = {
    "create table %s (%s);" format(
      tablename,  "\n  " + coldefs.reduceLeft(_ + ",\n  " + _ ))
  }
}
case class ColType(str:String, isPrimaryKey:Boolean =false, isAutoInc: Boolean = false, isUniqueKey: Boolean =false){
  def getDef = (List(str) ++ primaryDef ++ autoIncDef).reduceLeft(_ + " " + _)
  private def primaryDef = if (isPrimaryKey) List("primary key") else Nil
  private def autoIncDef = if (isAutoInc) List("auto_increment") else Nil
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
object Column{
}
object ColumnName{
  def unapply(c:Column) = Some((c.name,c.name))

}
class Table(val metadata:Metadata, val name:String, var columns:List[Column])
{
  def pk = primaryKey
  def primaryKey:Column = columns.filter(_.isPrimaryKey)(0)
  override def toString:String = "Table(%s)" format(name)

  def createTable = DDLUtil.createTable(name, columns.map(_.getDef))
  def insertSQL(xs: List[(String,RawVal)]): anorm.SimpleSql[anorm.Row] = {
    val s = """insert into %s (%s) values (%s)  ;""" format(
      name,
      (for ((x,_) <- xs) yield x).reduceLeft(_ + "," +_),
      (for ((x,_) <- xs) yield "{%s}" format(x)).reduceLeft(_ + "," +_)
    )
    SQL(s).on((for ((x,y) <- xs) yield (x, y.toParameterValue)).toList : _*)
  }
  def insertRows(xss: List[(String,RawVal)]*)(implicit connection: java.sql.Connection) =
	for (xs <- xss) yield insertSQL(xs).execute()
	
  def apply(colname:String):Column = columns.find(_.name == colname).get
}

object MockMetadata extends Metadata{
	def all = List()
}

object Table{
  def apply(name:String) = {
	new Table(MockMetadata, name, List())
}
  def apply(name:String,columns:List[Column])(implicit metadata:Metadata) = new Table(metadata, name, columns)
  
}
