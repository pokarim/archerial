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

trait AbstractArrow {
  this:Arrow => ;
  
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
	apply((UnitObject,UnitCol))._2
  }

  def apply(pred: (Object,QueryExp)):(Object,QueryExp) = ((pred,this) : @unchecked) match {
	case ((UnitObject,UnitCol), AllOf(obj)) => {
	  (obj, Col(obj.getColNode()))
	}
	case ((predObj , predCol:Col),AllOf(obj))=>{
	  val ColNode(table:TableNode,column) = obj.getColNode()
	  obj -> Col(
		ColNode(CrossJoin(table.table,predCol.table),column))
	  //obj -> Col(colNode)
	}

	case (pred, self: Identity) => {
	  self match{
	  case Identity(obj@ColObject(table,column)) =>{
		ColArrow(table, ColObject(table, column),
				 ColObject(table, column),
				 column,column)(pred)
	  }
	}}
	case ((_, pred:Col), self@ColArrow(table, _, cod, dcol, ccol)) => {
	  (cod, Col(JoinNode.joinWith(table, pred, dcol), ccol))
	}

	case (pred ,self@Composition(left, right)) => 
	  right(left(pred))

	case (pred@(obj,_), left =:= right) =>{
	  val (objL, l) = left(pred)
	  val (objR, r) = right(pred)
	  (obj,OpExps.=:= (l,r))
	}

	case (pred@(obj,_), left * right) =>{
	  val (objL, l) = left(pred)
	  val (objR, r) = right(pred)
	  (obj,OpExps.* (l,r))
	}

	case ((obj, col:Col), arr@Const(x)) => {
	  (arr.getObject, ConstCol(ConstantColExp(col.colNode.table, x)))
	}

	case (pred@(obj,Col(ColNode(_,_)))  , Filter(cond)) => {
	  val Col(ColNode(t1,_)) = pred._2
	  val pred2@(obj:ColObject, Col(ColNode(cTable, cCol))) = pred._1.id(pred)
	  cTable match {
		case x:JoinNode => {
		  val (_, condExp) = cond(pred2)
		  val condf = (node:JoinNode) =>
			cond(obj,Col(ColNode(node,cCol)))._2
		  val j = x.copy(condf=Some(condf))
		  import QueryExpTools.getTableExps
		  val trunk = (j :: getTableExps(cTable)).toSet
		  val branch = getTableExps(condf(j)).filter(!trunk(_))
		  if (branch.nonEmpty)
			Filter(Any(cond)).apply(pred)
		  else
			(obj,Col(j, obj.column))
		}
		case x =>{
		  val (_,condExp) = cond(pred2)
		  (obj,Col(WhereNode(cTable, condExp), obj.column ))
		}
	  }
	}

	case (pred  , Any(cond)) => {
	  val (_, pcol@Col(ColNode(cTable, cCol))) = pred
	  val (_,condExp ) = cond(pred)
	  (BoolObject,Exists(cTable, condExp))
	}

	// case (pred@(UnitObject,_)  , Sum(col)) => {
	//   val g = GroupByNode(UnitTable,Col(UnitTable.pk))
	//   val inner = Col(ColNode(g,UnitTable.pk.column))
	//   val (ro, rc:Col) = col(UnitObject -> inner)
	//   ro -> queryexp.SumQExp(UnitTable, rc)
	// }
	case (pred  , Sum(col)) => {
	  val (obj,//:ColObject, 
		   keycol@Col(ColNode(cTable, cCol))) = pred
	  val g = GroupByNode(cTable,keycol)
	  val inner = Col(ColNode(g,cCol))
	  val r@(_, valcol@Col(_) ) = col(obj -> inner)
	  IntObject -> queryexp.SumQExp(g,valcol)
	}

	case (pred@(_,Col(ColNode(_,_)))  , NonNull(cond)) => {
	  val (_, pcol@Col(ColNode(cTable, cCol))) = pred
	  val (obj,condExp@Col(ColNode(c2,_)) ) = cond(pred)
	  BoolObject -> queryexp.NonNullQExp(condExp)
	}


 	case (pred , self@Tuple(arrows)) =>{
	  val xs = arrows.map(_(pred));
	  TupleObject(xs.map(_._1)) -> 
	  NTuple(xs.map(_._2))
	}

 	case (pred@(obj,col) , self@NamedTuple(namedarrows)) =>{
	  val xs = for {(s,arr) <- namedarrows
					val (obj,c) = arr(pred)
				  } 
			   yield (s,c,obj)
	  TupleObject(xs.map(_._3)) -> 
	  NamedTupleQExp(col
		,xs.map{case (x,y,_)=>(x,y)})
	}
  }

  def =:=(right:Arrow) = {OpArrows.=:=(this,right)}
  def >>>(other:Arrow) = Composition.gen(this,other)
  def *(right:Arrow) = {OpArrows.*(this,right)}
  
  def isIdentity = false
}
