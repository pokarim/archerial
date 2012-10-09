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

sealed trait Arrow {
  import OpArrows._

  def eval(sp:ValueExp => List[TableTree]=SimpleGenTrees.gen)(implicit connection: java.sql.Connection):Seq[Value] = {
	valueExp.eval(sp)
  }
  def dummy = "abc"
  def dom:Object
  def cod:Object
  def unary_~ : Arrow
  def domcol:Option[Column]

  def getTableUnit :(Object,ValueExp) = dom match {
	case obj : ColObject => (obj,Col(obj.getColNode()))
  }

  def valueExp():ValueExp = 
	apply(getTableUnit)._2

  def apply(pred: (Object,ValueExp)):(Object,ValueExp) = ((pred,this) : @unchecked) match {
	case (pred, self: IdentityArrow) => {
	  self match{
	  case IdentityArrow(obj@ColObject(table,column)) =>{
		ColArrow(table, ColObject(table, column),
				 ColObject(table, column),
				 column,column)(pred)
	  }
	  case _ => pred
	}}
	case ((_, pred:Col), self@ColArrow(table, _, cod, dcol, ccol)) => {
	  (cod, Col(JoinNode.joinWith(table, pred, dcol), ccol))
	}

	case (pred ,self@ComposeArrow(left, right)) => 
	  right(left(pred))

	case (pred@(obj,_), left =:= right) =>{
	  val (objL, l) = left(pred)
	  val (objR, r) = right(pred)
	  (obj,OpExps.=:= (l,r))
	}

	case (_ ,arr@ConstantArrow(x)) => (arr.getObject,ConstantExp(x))

	case (pred  , FilterArrow(cond)) => {
	  val pred2@(obj:ColObject, Col(ColNode(cTable, cCol))) = pred._1.id(pred)
	  val (_,condExp) = cond(pred2)
	  (obj,Col(WhereNode(cTable, condExp), obj.column ))
	}

 	case (pred , self@TupleArrow(arrows)) =>{
	  val xs = arrows.map(_(pred));
	  TupleObject(xs.map(_._1)) -> 
	  NTuple(xs.map(_._2))
	}
  }


  def =:=(right:Arrow) = {new OpArrows.=:=(this,right)}
  
  def >>>(other:Arrow) = ComposeArrow.gen(this,other)
  def isIdentity = false
}

trait EndoMap extends Arrow{
  def domcol: Option[Column] = dom.columnOption
  def cod = dom
  def unary_~ : Arrow = this
  def codcol:Option[Column] = cod.columnOption
}


object OpArrows{
  case class =:=(left:Arrow, right:Arrow) extends Arrow
  {
	def dom = left.dom
	def cod = BoolObject
	def domcol:Option[Column] = left.domcol
	def codcol:Option[Column] = None
	def unary_~ : Arrow = throw new Exception("=:=.unary") 
  }

}
case class ConstantArrow(x: RawVal) extends Arrow {
  def getObject:Object = {
	import RawVal._
	  x match {
		case _ :Int => IntObject
		case _ :Str => StrObject
	  }
  }
	def dom = getObject
	def cod = getObject
	def domcol:Option[Column] = None
	def codcol:Option[Column] = None
	def unary_~ : Arrow = this
}

case class FilterArrow(cond:Arrow) extends EndoMap{
  require(cond.cod == BoolObject)
  def dom = cond.dom
}

case class IdentityArrow(dom:Object) extends EndoMap{
  override def isIdentity = true
}

object ColArrow{

  def apply(table:Table, dom:Object, cod:Object, domcolname :String, codcolname :String):ColArrow =
	apply(table, dom, cod, table(domcolname), table(codcolname))

  def apply(dom:Object, cod:Object, domcolname :String, codcolname :String)(implicit table:Table):ColArrow =
  	apply(table, dom, cod, table(domcolname), table(codcolname))
						  
}

case class ColArrow(table:Table, dom:Object, cod:Object,
						_domcol :Column, _codcol :Column) extends Arrow{
  def domcol = Some(_domcol)
  def codcol = Some(_codcol)
  def name = _domcol.name ++ " -> " ++ _codcol.name
  def unary_~ : Arrow = ColArrow(table,cod,dom, _codcol,_domcol)
}


case class TupleArrow(arrows: List[Arrow]) extends Arrow {

  def domcol: Option[Column] = arrows.head.domcol
  def unary_~ = {assert(false);null} //TupleArrow(arrows.map(~_)) 
  def codcol = None
  def dom = arrows.head.dom
  def cod = arrows.head.cod

}
object TupleArrow{
  def apply(arrows: Arrow*) = new TupleArrow(arrows.toList)
}

 
case class ComposeArrow(left:Arrow,right:Arrow) extends Arrow {

  def domcol: Option[Column] = left.domcol
  def dom = left.dom
  def cod = right.cod
  //def codcol:Option[Column] = right.codcol
  override def toString:String = "(%s ++ %s)" format (left,right)
  def unary_~ : Arrow = (~right)>>>(~left)
  require (left.cod == right.dom,(left.cod ,"must be", right.dom))
}
object ComposeArrow{
  def gen(left:Arrow,right:Arrow):Arrow = if (right.isIdentity) left else
	left match {
	  case _ : IdentityArrow => right
      case ComposeArrow(ll,lr) => gen(ll, gen(lr,right))
      case _ => new ComposeArrow(left,right)
	}
}



