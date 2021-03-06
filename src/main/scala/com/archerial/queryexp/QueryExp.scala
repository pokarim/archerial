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

trait Columnable extends QueryExp{
  def qexpCol = QExpCol(this)
  def eval(vcol:ColExp, values:SeqValue, getter:RowsGetter ):SeqValue = 
  ColEvalTool.eval(qexpCol, vcol, values, getter)
  def row2value(row:Row ): Value = row.d(qexpCol)
  def getDependentCol():Stream[ColExp] = Stream(qexpCol)
}

object BinOp {
  def unapply(b:BinOp):Option[(QueryExp,QueryExp)] = Some(b.left -> b.right)
}
trait BinOp extends QueryExp{
  val left:QueryExp
  val right:QueryExp
  override def constants:Seq[ConstantQueryExp] = 
    left.constants ++ right.constants

  
  def SQLOpString:String
  
  def getRawValue(left:RawVal,right:RawVal):RawVal

  
  def getSQL(map: TableIdMap):String = {
    val l = left.getSQL(map)
    val r = right.getSQL(map)
    l + " "+ SQLOpString + " " + r
  }
}

trait ColumnableBinOp extends Columnable with BinOp {
  override def getDependentCol():Stream[ColExp] = 
    qexpCol #:: // TODO
    left.getDependentCol() ++ right.getDependentCol()
}

trait NonColumnableBinOp extends BinOp {
  override def eval(col:ColExp, values:SeqValue, getter:RowsGetter ):SeqValue = {
        for {Val(l,ln) <- left.eval(col,values,getter)
             Val(r,rn) <- right.eval(col,values,getter)
             val v = getRawValue(l,r)}
        yield Val(v, ln * rn)
      }

  override def row2value(row:Row ): Value =
      (left.row2value(row),right.row2value(row)) match {
        case (Val(l,ln),Val(r,rn)) =>
          Val(getRawValue(l,r), ln * rn)} 
}

object OpExps {
  case class *(left:QueryExp, right:QueryExp) extends ColumnableBinOp{
    def getRawValue(left:RawVal,right:RawVal):RawVal =
      (left,right) match{
        case (RawVal.Int(x),RawVal.Int(y)) =>
          RawVal.Int(x * y)
        case (RawVal.Long(x),RawVal.Long(y)) =>
          RawVal.Long(x * y)
      }
    def SQLOpString:String = "*"
  }

  case class =:=(left:QueryExp, right:QueryExp) extends ColumnableBinOp{

    def getRawValue(left:RawVal,right:RawVal):RawVal =
      RawVal.Bool(left == right)
    
    def SQLOpString:String = "="
  }

  case class And(left:QueryExp, right:QueryExp) extends ColumnableBinOp{

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

  def getSQL(map:TableIdMap):String = {
    if (map.constMap.contains(this))
      "{%s}" format(map.constMap(this))
    else
      rawVal.toSQLString
  }
}
case class ConstantExp(rawVal:RawVal) extends ConstantQueryExp {
  def getDependentCol():Stream[ColExp] = Stream.Empty
  def eval(col:ColExp, values:SeqValue, getter:RowsGetter ):SeqValue =
	VList(Seq(Val(rawVal,1)))

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

  override def eval(vcol:ColExp, values:SeqValue, getter:RowsGetter) = 
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
    else
      "error"

  override def toShortString:String = column.name
  override def toString:String = "Col(%s:: %s)" format(column.name, table)

  override def evalCol(colExp:ColExp):ColExp = colNode
  override def eval(vcol:ColExp, values:SeqValue, getter:RowsGetter ):SeqValue = 
  ColEvalTool.eval(colNode, vcol, values, getter)
}

case class NamedTupleQExp(keycol:QueryExp,exps :List[(String,QueryExp)]) extends QueryExp with TupleExpBase {
  def keyExp = keycol
  def valExps = exps.map(_._2)

  def eval(colExp:ColExp, values:SeqValue, getter:RowsGetter ): SeqValue = {
    val ks = keyExp.eval(colExp,values,getter).distinct
    val kcol = keyExp.evalCol(colExp)
    for {k <- ks}
    yield {
      NamedVTuple(("__id__", VList(k)) :: 
      (for {(name,vexp) <- exps}
       yield (name,
              VList(vexp.eval(kcol,VList(Seq(k)),getter).toSeq))
            ) :_*)
    }
  }

}

case class NTuple(exps :List[QueryExp]) extends QueryExp with TupleExpBase{
  assert(!exps.isEmpty,"!exps.isEmpty")
  def eval(colExp:ColExp, values:SeqValue, getter:RowsGetter ): SeqValue = {
    val ks = keyExp.eval(colExp,values,getter).distinct
    val kcol = keyExp.evalCol(colExp)
    for {k <- ks}
    yield {
      VTuple(VList(k) :: 
      (for {vexp <- valExps}
       yield VList(vexp.eval(kcol,VList(Seq(k)),getter).toSeq )) :_*)
    }
  }

  def keyExp = exps.head
  def valExps = exps.tail

}
case class NonNullQExp(col:QueryExp) extends Columnable{
  def getSQL(map: TableIdMap):String =    {
    "(%s is not null)" format col.getSQL(map)
  }
  override def constants:Seq[ConstantQueryExp] = col.constants
}

trait AggregateFuncQExp extends Columnable{
  override def eval(vcol:ColExp, values:SeqValue, getter:RowsGetter ):SeqValue = {
    val vs = ColEvalTool.eval(
	  qexpCol, vcol, values, getter,false)
    if (vs.isEmpty) 
      VList(Val(RawVal.Int(0)))
    else
      for {v <- vs}
      yield if (v.nonNull) v 
			else Val(RawVal.Int(0))
  }
  override def constants:Seq[ConstantQueryExp] = valcol.constants
  val valcol:QueryExp
  val source:GroupByNode
}

object AggregateFuncQExp{
  def unapply(x:AggregateFuncQExp):Option[(GroupByNode,QueryExp)] =
    Some((x.source,x.valcol))
}

case class SumQExp(source:GroupByNode,valcol:QueryExp) extends AggregateFuncQExp{
  def getSQL(map: TableIdMap):String =
    "sum(%s)" format valcol.getSQL(map)
}

case class AvgQExp(source:GroupByNode,valcol:QueryExp) extends AggregateFuncQExp{
  def getSQL(map: TableIdMap):String =
    "avg(%s)" format valcol.getSQL(map)
}


case class Exists(source:TableExp,cond:QueryExp) extends Columnable{
  val col :QueryExp = 
         ConstCol(ConstantColExp(
           WhereNode(source, cond),
           RawVal.Int(1)))

  override def constants:Seq[ConstantQueryExp] = cond.constants

  override lazy val colList = List(col.colList.head)
  def getSQL(map: TableIdMap):String =    {
    val trees = SimpleGenTrees.oneTree(col, source).children
    require(trees.length==1, trees.length) // TODO cross join
    val tree = trees.head
    val colList = List(col.colList.head)
    val select = SelectGen.gen(tree, colList, Some(map))
    val sql = select.getSQL(None)
    "exists (%s)" format sql._1
  }
}
