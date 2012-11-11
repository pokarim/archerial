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

package com.archerial.arrows

import com.archerial.objects._
import com.archerial._
import com.archerial.queryexp._
import OpArrows._
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._
import java.sql.Connection
import QueryExpTools.getTableExps
import scalaz.{Value => _, _}, Scalaz._

import com.archerial.utils.StateUtil.{forS,forFlatS,return_}
import RawVal.GeneratedId
case class Update(s:Stream[(ColArrow,Value,Value)],nextId:GeneratedId=GeneratedId.zero){
}

object Update{
  val empty = Update(Stream.empty)
  def append(arrow:ColArrow,id:Value,v:Value) =
	modify[Update]((u) => u.copy(s=(arrow, id, v) #:: u.s))

  def genId() =
	for {u <- init[Update]
		 _ <- modify[Update]((u) => u.copy(nextId=u.nextId.next()))
	   } yield Val(u.nextId,1)
	//modify[Update]((u) => u.copy(s=(arrow, id, v) #:: u.s))
  def updateAndAdd(arrow:ColArrow,id:Value,v:SeqValue):State[Update, Unit]=
	for {vs <- arrow.update(v)
		 _ <- append(arrow,id,vs)}
	yield ();

}
object UpdateTools{
  import Update.append
  def namedTupleUpdate(values:SeqValue,arrows:Seq[(String,Arrow)]) :State[Update, SeqValue] = {
	forFlatS(values){
	  case v:NamedVTuple
	  if v.contains("__id__") || v.contains("__new__")=> {
		val getId = 
		  if (v.contains("__id__")) {
			val VList(Seq(id)) = v("__id__")
			return_[Update,Option[Value]](Some(id))}
		  else Update.genId().map(Some(_))
		for {id <- getId
			 _ <- forS(for {(n,a) <- arrows} yield (id,a,v.get(n))){
			   case (Some(id),arrow:ColArrow, Some(vs:VList)) =>
				 append(arrow, id, vs)
			   case (Some(id),>>>(left:ColArrow, right), Some(vs:VList)) =>
				 right.update(vs) >>= {append(left, id, _)}
			   case (_,x,Some(vs:VList)) => x.update(vs)
			   case _ => {pprn("hoge");init[Update]}
			 }} yield Seq(id.getOrElse(Val(RawVal.Null,1)))
	  }
	  case v => return_(Seq(v))
	}
  }
  
}
trait AbstractArrow {
  this:Arrow => ;
  type Conn = Connection
  def eval(sp:QueryExp => List[TableTree]=SimpleGenTrees.gen)(implicit connection: java.sql.Connection):Seq[Value] = {
	queryExp.eval(sp)
  }

  def dummy = "abc"
  def dom:Object
  def cod:Object
  def unary_~ : Arrow

  def queryExp():QueryExp = {
	require(
	  this.dom == UnitObject,
	  "queryExp() needs this.dom == UnitObject but %s.dom is %s"
	  format(this,this.dom))
	apply(UnitCol)
  }
  
  def update(values:SeqValue):State[Update, SeqValue] = {
	  this match {
		case Composition(left,right) =>
		  right.update(values)
		case NamedTuple(arrows) =>
		  UpdateTools.namedTupleUpdate(values,arrows)
		case _ => return_(values)
	  }
  }
  
  def apply(pred: QueryExp):QueryExp = 
	((pred, this) : @unchecked) match {
	case (UnitCol, AllOf(obj)) => Col(obj.getColNode())

	case (predCol:Col,AllOf(obj))=>{
	  val ColNode(table:TableNode,column) = obj.getColNode()
	  Col(ColNode(CrossJoin(table.table, predCol.table), column))
	}

	case (predcol, Identity(obj@ColObject(table,column))) => 
		ColArrow(table, ColObject(table, column),
				 ColObject(table, column),
				 column,column)(predcol)

	case (pred:Col, self@ColArrow(table, _, _, dcol, ccol)) => 
	  Col(JoinNode.joinWith(table, pred, dcol), ccol)

	case (pred ,self@Composition(l, r)) => r(l(pred))

	case (pred, self@OpArrow(l,r)) => self.qexp(l(pred),r(pred))

	case (col:Col, arr@Const(x)) =>
	  ConstCol(ConstantColExp(col.colNode.table, x))

	case (pred, self@Filter(cond)) => {
	  val pred2@Col(ColNode(cTable, cCol)) = dom.id(pred)
	  cTable match {
		case x:JoinNode => {
		  val condf = (node:JoinNode) => cond(Col(ColNode(node,cCol)))
		  val j = x.copy(condf=Some(condf))
		  val trunk = (j :: getTableExps(cTable)).toSet
		  val branch = getTableExps(condf(j)).filter(!trunk(_))
		  if (branch.nonEmpty) Filter(Any(cond)).apply(pred2)
		  else Col(j, self.dom.column)
		}
		case _ =>
		  Col(WhereNode(cTable, cond(pred2)), self.dom.column )
	  }
	}

	case (keycol@Col(_), func@AggregateFunc(arr)) => {
	  val g = GroupByNode(keycol.colNode.table, keycol)
	  val valcol = arr(Col(keycol.colNode.copy(table=g)))
	  func.qexp(g,valcol)
	}

	case (pred@Col(_), Any(cond)) => 
	  Exists(pred.colNode.table, cond(pred))

	case (pred, NonNull(cond)) => queryexp.NonNullQExp(cond(pred))

 	case (pred, self@Tuple(arrows)) => NTuple(arrows.map(_(pred)))

 	case (pred, self@NamedTuple(ns)) =>
	  NamedTupleQExp(pred, ns.map{case (s,a) => (s,a(pred))})
  }

  def =:=(right:Arrow) = {OpArrows.=:=(this,right)}
  def >>>(other:Arrow) = Composition.gen(this,other)
  def *(right:Arrow) = {OpArrows.*(this,right)}
  
  def isIdentity = false
}
