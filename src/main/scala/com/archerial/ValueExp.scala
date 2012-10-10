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
import scala.collection.immutable
import implicits._
import SeqUtil.groupTuples

sealed trait ValueExp extends AbstractValueExp

object BinOp {
  def unapply(b:BinOp):Option[(ValueExp,ValueExp)] = Some(b.left -> b.right)
}
trait BinOp extends ValueExp{
  val left:ValueExp
  val right:ValueExp
  def getDependentCol():Stream[ColExp] = left.getDependentCol() ++ right.getDependentCol()
  
  def SQLOpString:String
  
  def getRawValue(left:RawVal,right:RawVal):Option[RawVal]

  def eval(col:ColExp, values:Seq[Value], getter:RowsGetter ):Seq[Value] = {
	  for {Val(l,ln) <- left.eval(col,values,getter)
		   Val(r,rn) <- right.eval(col,values,getter)
		   v <- getRawValue(l,r).toList}
	yield Val(v, ln * rn)
	}

  def rows2value(rows:Seq[Row] ): Seq[Value] =
	  for {Val(l,ln) <- left.rows2value(rows)
		   Val(r,rn) <- right.rows2value(rows)
		   v <- getRawValue(l,r).toList} 
	  yield Val(v,ln * rn)
  
  
  def getSQL(map: TableIdMap):String = {
	val l = left.getSQL(map)
	val r = right.getSQL(map)
	l + " "+ SQLOpString + " " + r
  }
}

object OpExps {
  case class =:=(left:ValueExp, right:ValueExp) extends BinOp{

	def getRawValue(left:RawVal,right:RawVal):Option[RawVal] =
	  Some(RawVal.Bool(left == right))
	
	def SQLOpString:String = "="
  }

  case class And(left:ValueExp, right:ValueExp) extends BinOp{

	def getRawValue(left:RawVal,right:RawVal):Option[RawVal] =
	  (left,right) match {
		case (RawVal.Bool(l),RawVal.Bool(r)) =>
		  Some(RawVal.Bool(l && r))
	  }
	def SQLOpString:String = "AND"
  }
}
case class ConstantExp(x:RawVal) extends ValueExp {
  def getDependentCol():Stream[ColExp] = Stream.Empty
  def eval(col:ColExp, values:Seq[Value], getter:RowsGetter ):Seq[Value] = List(Val(x,1))

  override def rows2value(rows:Seq[Row] ): Seq[Value] = {
	List(Val(x,1))
  }

  def getSQL(map: TableIdMap):String = 	x.toSQLString
}
object Col{
  def apply(table: TableExp, column: Column) = 
	new Col(ColNode(table,column))
}

object UnitCol extends Col(UnitTable.pk)

case class ConstCol(constColExp:ConstantColExp) extends ValueExp{
  override def eval(vcol:ColExp, values:Seq[Value], getter:RowsGetter) = 
  ColEvalTool.eval(
	constColExp, constColExp.table, vcol, values, getter)

  def value = Val(constColExp.value,1)
  def getSQL(map:TableIdMap):String = 
	constColExp.getSQL(map)
  def getDependentCol():Stream[ColExp] = 
	Stream.Empty 
  def rows2value(rows:Seq[Row] ): Seq[Value] =
	for (_ <- rows) yield value

}
case class Col(colNode: ColNode) extends ValueExp{
  def getDependentCol():Stream[ColExp] = 
	if (colNode == pk)
	  Stream(pk)
	else
	  Stream(pk,colNode)

  def table = colNode.table
  def column = colNode.column

  val pk = table.primaryKeyCol

  override def rows2value(rows:Seq[Row] ): Seq[Value] = {
	if (this == pk)
	  for (row <- rows) yield row.d(colNode)
	else {
	  val kvs = for (row <- rows; val pkv = row.d(pk); if !pkv.isNull)
				yield (pkv, row.d(colNode));
	  SeqUtil.distinctBy1stAndGet2nd(kvs).toList
	}
  }
  import StateUtil.forS
			 
  def getSQL(map: TableIdMap):String =	"%s.%s" format(map.gets(table).get, column.name)

  override def toShortString:String = column.name
  override def toString:String = "Col(%s:: %s)" format(column.name, table)

  override def evalCol(colExp:ColExp):ColExp = colNode
  override def eval(vcol:ColExp, values:Seq[Value], getter:RowsGetter ):Seq[Value] = 
  ColEvalTool.eval(colNode, colNode.table, vcol, values, getter)
}

object ColEvalTool{
  def eval(colExp:ColExp, table:TableExp, vcol:ColExp, values:Seq[Value], getter:RowsGetter ): Seq[Value] = {
	val xs = getter.colInfo.table_tree(table)
	val tree = getter.colInfo.table_tree.one(table)
	val vtrees = getter.colInfo.table_tree(vcol.tables.head)
	val (ptree,pcol,pvalues) =
	if (!vtrees.contains(tree) ){
	  val ptree = 
		getter.colInfo.table_tree.one(
		  tree.node.directParent.get)
	  val pcol = tree.node.rootCol.asInstanceOf[ColNode]//TODO
	  val ptree2 = getter.colInfo.table_tree.one(pcol.tables.head)
	  assert(ptree == ptree2,"ptree == ptree2")
	  val pvals = Col(pcol).eval(vcol,values,getter)
	  val pcol2 = Col(pcol).evalCol(pcol)
	  (ptree,pcol2,pvals)
	}else{
	  (tree,vcol,values)
	}
	val prows = getter.tree2rows(tree)
	val c2v2r = 
	  if (getter.t2c2v2r(table).contains(pcol) 
		  || pcol == UnitTable.pk)
		getter.t2c2v2r(table)
	  else
		getter.t2c2v2r(pcol.tables.head)
	
	if (pcol == UnitTable.pk){
	  if(colExp ==table.pk){
		c2v2r(colExp).keys.toSeq
	  }else{
		c2v2r(table.pk).values.toSeq.flatMap(_.map(_.d(colExp)))
	  }
	} else {
	  for {pv <- pvalues;
		   row <- c2v2r(pcol).getOrElse(pv,Nil)
		   val v = row.d(colExp)
		   if v.nonNull}
	  yield v
	}
  }
}

case class NTuple(exps :List[ValueExp]) extends ValueExp{
  assert(!exps.isEmpty,"!exps.isEmpty")
  override def eval(colExp:ColExp, values:Seq[Value], getter:RowsGetter ): Seq[Value] = {
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
  def getDependentCol():Stream[ColExp] = 
	exps.toStream.flatMap(_.getDependentCol())

  override def rows2value(rows:Seq[Row] ): Seq[Value] = {
	val kvs = for {row <- rows
				   k <- exps(0).rows2value(List(row)).headOption.toList
				   if !k.isNull}
			  yield (k,row)
	for ((key,cs) <- groupTuples(kvs).toSeq) yield
	  VTuple( key.one ::
			 (for (exp <- exps.tail)
			  yield VList(exp.rows2value(cs).toSeq :_*)) :_*)
  }
  import StateUtil.reduceStates

  def getSQL(map: TableIdMap):String = {
	throw new Exception("invalid operation")
  }
}
