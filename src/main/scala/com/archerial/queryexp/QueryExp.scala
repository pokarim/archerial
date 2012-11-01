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
import com.archerial.objects.ColObject
import com.archerial.utils._
import scala.collection.immutable
import SeqUtil.groupTuples

import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._

sealed trait QueryExp extends AbstractQueryExp

object BinOp {
  def unapply(b:BinOp):Option[(QueryExp,QueryExp)] = Some(b.left -> b.right)
}
trait BinOp extends QueryExp{
  val left:QueryExp
  val right:QueryExp
  override def constants:Seq[ConstantQueryExp] = 
	left.constants ++ right.constants

  def getDependentCol():Stream[ColExp] = left.getDependentCol() ++ right.getDependentCol()
  
  def SQLOpString:String
  
  def getRawValue(left:RawVal,right:RawVal):RawVal

  def eval(col:ColExp, values:Seq[Value], getter:RowsGetter ):Seq[Value] = {
	  for {Val(l,ln) <- left.eval(col,values,getter)
		   Val(r,rn) <- right.eval(col,values,getter)
		   val v = getRawValue(l,r)}
	  yield Val(v, ln * rn)
	}

  def row2value(row:Row ): Value =
	(left.row2value(row),right.row2value(row)) match {
	  case (Val(l,ln),Val(r,rn)) =>
		Val(getRawValue(l,r), ln * rn)} 
  
  def getSQL(map: TableIdMap):String = {
	val l = left.getSQL(map)
	val r = right.getSQL(map)
	l + " "+ SQLOpString + " " + r
  }
}

object OpExps {
  case class *(left:QueryExp, right:QueryExp) extends BinOp{

	def getRawValue(left:RawVal,right:RawVal):RawVal = (left,right)
	  match{
		case (RawVal.Int(x),RawVal.Int(y))
		=> RawVal.Int(x * y)
	  }
	def SQLOpString:String = "*"
  }

  case class =:=(left:QueryExp, right:QueryExp) extends BinOp{

	def getRawValue(left:RawVal,right:RawVal):RawVal =
	  RawVal.Bool(left == right)
	
	def SQLOpString:String = "="
  }

  case class And(left:QueryExp, right:QueryExp) extends BinOp{

	def getRawValue(left:RawVal,right:RawVal):RawVal =
	  (left,right) match {
		case (RawVal.Bool(l),RawVal.Bool(r)) =>
		  RawVal.Bool(l && r)
	  }
	def SQLOpString:String = "AND"
  }
}
trait ConstantQueryExp extends QueryExp {
  def rawVal:RawVal
  override def constants:Seq[ConstantQueryExp] = List(this)
  // def getSQLandParmas(map: TableIdMap):(String,Seq[(String,RawVal)]) =
  // 	(getSQL(map),List( -> rawVal))

  def getSQL(map:TableIdMap):String = {
	if (map.constMap.contains(this))
	  "{%s}" format(map.constMap(this))
	else
	  rawVal.toSQLString
  }
}
case class ConstantExp(rawVal:RawVal) extends ConstantQueryExp {
  def getDependentCol():Stream[ColExp] = Stream.Empty
  def eval(col:ColExp, values:Seq[Value], getter:RowsGetter ):Seq[Value] = List(Val(rawVal,1))

  override def row2value(row:Row ): Value =
	Val(rawVal,1)
}
object Col{
  def apply(table: TableExp, column: Column) = 
	new Col(ColNode(table,column))
}

object UnitCol extends Col(UnitTable.pk)

case class ConstCol(constColExp:ConstantColExp) extends ConstantQueryExp{
  def rawVal:RawVal = constColExp.value

  override def eval(vcol:ColExp, values:Seq[Value], getter:RowsGetter) = 
  ColEvalTool.eval(
	constColExp, vcol, values, getter)

  def value = Val(constColExp.value,1)
  def getDependentCol():Stream[ColExp] = 
	Stream.Empty 
  def row2value(row:Row ): Value = value

}
case class Col(colNode: ColNode) extends QueryExp{
  def getDependentCol():Stream[ColExp] = 
	if (colNode == pk)
	  Stream(pk)
	else
	  Stream(pk,colNode)

  def table = colNode.table
  def column = colNode.column

  val pk = table.primaryKeyCol

  def row2value(row:Row ): Value = {
	val rows = List(row)
	(if (this == pk)
	  for (row <- rows) yield row.d(colNode)
	else {
	  val kvs = for (row <- rows; val pkv = row.d(pk); if !pkv.isNull)
				yield (pkv, row.d(colNode));
	  SeqUtil.distinctBy1stAndGet2nd(kvs).toList
	}).head
  }
  def getSQL(map: TableIdMap):String =	
	if (map.map.contains(table))
	  "%s.%s" format(map.gets(table).get, column.name)
	else{
	  "error"
	}

  override def toShortString:String = column.name
  override def toString:String = "Col(%s:: %s)" format(column.name, table)

  override def evalCol(colExp:ColExp):ColExp = colNode
  override def eval(vcol:ColExp, values:Seq[Value], getter:RowsGetter ):Seq[Value] = 
  ColEvalTool.eval(colNode, vcol, values, getter)
}

case class NamedTupleQExp(keycol:QueryExp,exps :List[(String,QueryExp)]) extends QueryExp with TupleExpBase {
  def keyExp = keycol//dom.id
  def valExps = exps.map(_._2)

  def eval(colExp:ColExp, values:Seq[Value], getter:RowsGetter ): Seq[Value] = {
	val ks = keyExp.eval(colExp,values,getter)
	val kcol = keyExp.evalCol(colExp)
	for {k <- ks}
	yield {
	  NamedVTuple(("__id__", VList(k)) :: 
	  (for {(name,vexp) <- exps}
	   yield (name,
			  VList(vexp.eval(kcol,List(k),getter).toSeq :_* ))
			) :_*)
	}
  }

}

case class NTuple(exps :List[QueryExp]) extends QueryExp with TupleExpBase{
  assert(!exps.isEmpty,"!exps.isEmpty")
  def eval(colExp:ColExp, values:Seq[Value], getter:RowsGetter ): Seq[Value] = {
	val ks = keyExp.eval(colExp,values,getter)
	val kcol = keyExp.evalCol(colExp)
	for {k <- ks}
	yield {
	  VTuple(VList(k) :: 
	  (for {vexp <- valExps}
	   yield VList(vexp.eval(kcol,List(k),getter).toSeq :_* )) :_*)
	}
  }

  def keyExp = exps.head
  def valExps = exps.tail

}
trait Columnable extends QueryExp{
  def root:TableExp
  def qexpCol = QExpCol(root,this)
  def eval(vcol:ColExp, values:Seq[Value], getter:RowsGetter ):Seq[Value] = 
  ColEvalTool.eval(qexpCol, vcol, values, getter)
  def row2value(row:Row ): Value = row.d(qexpCol)

}

case class NonNullQExp(root:TableExp,col:QueryExp) extends Columnable{
  def getDependentCol():Stream[ColExp] = Stream(qexpCol)
  def getSQL(map: TableIdMap):String =	{
	"(%s is not null)" format col.getSQL(map)
  }
  override def constants:Seq[ConstantQueryExp] = col.constants
}

case class SumQExp(table:GroupByNode,valcol:Col) extends Columnable{
  val root:TableExp = valcol.table
  override def eval(vcol:ColExp, values:Seq[Value], getter:RowsGetter ):Seq[Value] = {
	val t = table//root
	val vs = ColEvalTool.eval(
	  qexpCol, vcol, values, getter,false)
	if (vs.isEmpty) 
	 Seq(Val(RawVal.Int(0)))
	else
	  for {v <- vs}
	  yield {
		if (v.nonNull) v 
		else Val(RawVal.Int(0))
	  }
  }
  def getDependentCol():Stream[ColExp] = Stream(qexpCol,
											  table.key.colNode)
  def getSQL(map: TableIdMap):String =	{
	"sum(%s)" format valcol.getSQL(map)
  }
  override def constants:Seq[ConstantQueryExp] = 
	valcol.constants
}


case class Exists(root:TableExp,cond:QueryExp) extends Columnable{
  val col :QueryExp = 
		 ConstCol(ConstantColExp(
		   WhereNode(root, cond),
		   RawVal.Int(1)))

  override def constants:Seq[ConstantQueryExp] = cond.constants

  def getDependentCol():Stream[ColExp] = Stream(qexpCol)

  override lazy val colList = List(col.colList.head)
  def getSQL(map: TableIdMap):String =	{
	val any = this
	val exp = col
	val trees = SimpleGenTrees.oneTree(exp, root).children
	require(trees.length==1,trees.length) // TODO cross join
	val tree = trees.head
	val colList = List(col.colList.head)
	val col2table = Rel.gen[ColExp,TableExp](
	  colList)((x:ColExp) => x.tables)
	val colInfo = TreeColInfo(col2tableOM, trees)
	val select = SelectGen.gen(tree,colList,Some(map))
	val sql = select.getSQL(None)
	"exists (%s)" format sql._1
  }


}
