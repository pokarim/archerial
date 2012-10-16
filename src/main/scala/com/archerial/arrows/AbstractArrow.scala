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

	case ((obj, col:Col), arr@Const(x)) => {
	  (arr.getObject, ConstCol(ConstantColExp(col.colNode.table, x)))
	}

	case (pred  , Filter(cond)) => {
	  val pred2@(obj:ColObject, Col(ColNode(cTable, cCol))) = pred._1.id(pred)
	  val (_,condExp) = cond(pred2)
	  (obj,Col(WhereNode(cTable, condExp), obj.column ))
	}

	case (pred  , Any(cond)) => {
	  val (obj:ColObject, pcol@Col(ColNode(cTable, cCol))) = pred
	  val (_,condExp : OpExps.=:=) = cond(pred)
	  (obj,Exists(cTable,condExp))
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

  def =:=(right:Arrow) = {new OpArrows.=:=(this,right)}
  def >>>(other:Arrow) = Composition.gen(this,other)
  
  def isIdentity = false
}
