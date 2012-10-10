package com.archerial

sealed trait ColExp {
  def getSQL(map: TableIdMap):String
  def parse(x:Any):RawVal = x match {
	case null => RawVal.Null
	case x:Int => RawVal.Int(x)
	case x:String => RawVal.Str(x)
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
  def isPrimaryKey:Boolean = getColNodes match {
	case List(ColNode(_,c)) if c.isPrimaryKey => true
	case _ => false
  }
}
case class ColNode(table: TableExp, column: Column) extends ColExp{
  def getTables:List[TableExp] = List(table)
  def getColNodes:List[ColNode] = List(this)
  def getSQL(map: TableIdMap):String = {
	"%s.%s" format(map(table), column.name)
  }
}

case class ConstantColExp(table: TableExp, value: RawVal) extends ColExp {
  def getTables:List[TableExp] = List(table)
  def getColNodes:List[ColNode] = Nil
  def getSQL(map: TableIdMap):String = 
	value.toSQLString
}
