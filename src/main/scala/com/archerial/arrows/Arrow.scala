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
import com.archerial._
import com.archerial.objects._
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._
import com.archerial.queryexp._

sealed trait Arrow extends AbstractArrow

trait ArrowGen extends Arrow
// trait StrictArrow extends ArrowGen {
// }

object Arrow{
  implicit def toConst(x:String):Arrow = Const(RawVal.Str(x))
}

case class AllOf(cod:ColObject) extends Arrow{
  def dom = UnitObject
  def unary_~ : Arrow = 
	throw new java.lang.UnsupportedOperationException(
	  "AllOf().inverse")
}
trait BoolUnaryOp extends ArrowGen{
  require(cond.cod == BoolObject)
  val cond:Arrow
  def dom = cond.dom
  def cod = BoolObject
  def unary_~ : Arrow = 
	throw new java.lang.UnsupportedOperationException(
	  "BoolUnaryOp().inverse")
}

trait UnaryColOp extends ArrowGen{
  val col:Arrow
  def dom = col.dom
  def unary_~ : Arrow = 
	throw new java.lang.UnsupportedOperationException(
	  "UnaryColOp().inverse")
}

case class All(cond:Arrow) extends BoolUnaryOp
case class Any(cond:Arrow) extends BoolUnaryOp

case class NonNull(col:Arrow) extends UnaryColOp{
  def cod = BoolObject
}
case class IsNull(col:Arrow) extends UnaryColOp{
  def cod = BoolObject
}
trait AggregateFunc extends UnaryColOp{
  def qexp:(GroupByNode,QueryExp) => Columnable
  val col:Arrow
}
object AggregateFunc{
  def unapply(x:AggregateFunc):Option[Arrow] = Some(x.col)
}
case class Sum(col:Arrow) extends AggregateFunc{
  def cod = IntObject
  def qexp = SumQExp
}

case class Avg(col:Arrow) extends AggregateFunc{
  def cod = IntObject
  def qexp = AvgQExp
}

trait EndoMap extends Arrow{
  def cod = dom
  def unary_~ : Arrow = this
}

object OpArrow{
  def unapply(x:OpArrow):Option[(Arrow,Arrow)] = 
	Some((x.left,x.right))
}
trait OpArrow extends Arrow{
  val left:Arrow
  val right :Arrow
  def qexp:(QueryExp,QueryExp) => QueryExp
  def unary_~ : Arrow = throw new Exception("OpArrow.unary")
  def dom = (left.dom,right.dom) match {
	case (c:ColObject ,_) => c
	case (_, c) => c
  }
}
object OpArrows{
  case class =:=(left:Arrow, right:Arrow) extends OpArrow
  {
	def cod = BoolObject
	def qexp = OpExps.=:=
  }

  case class *(left:Arrow, right:Arrow) extends OpArrow
  {
	def cod = left.cod
	def qexp = OpExps.*
  }

}
case class Const(x: RawVal) extends Arrow {
  def getObject:Object = {
	import RawVal._
	  x match {
		case _ :Int => IntObject
		case _ :Str => StrObject
	  }
  }
	def dom = UnitObject
	def cod = getObject
	def unary_~ : Arrow = this
}

case class Filter(cond:Arrow) extends EndoMap{
  require(cond.cod == BoolObject && cond.dom.isInstanceOf[ColObject])
  def dom:ColObject = cond.dom match {
	case c:ColObject => c
  }

}

case class Identity(dom:Object) extends EndoMap{
  override def isIdentity = true
}

object ColArrow{
  def apply(dom:ColObject, cod:ColObject):ColArrow = {
	require(dom.table == cod.table)
	new ColArrow(dom.table,dom,cod,dom.column,cod.column)
  }
  def apply(table:Table, dom:ColObject, cod:ColObject, domcolname :String, codcolname :String):ColArrow = {
	apply(table, dom, cod, table(domcolname), table(codcolname))
  }

  def apply(dom:ColObject, cod:ColObject, table:Table, domcolname :String, codcolname :String):ColArrow = {
	require(dom.table == cod.table)
	apply(table, dom, cod, table(domcolname), table(codcolname))
  }

  def apply(dom:ColObject, domcolname :String, codcolname :String):ColArrow = {
	val table:Table = dom.table
	val cod = dom
  	apply(table, dom, cod, table(domcolname), table(codcolname))
  }

  def apply(dom:ColObject, cod:ColObject, domcolname :String, codcolname :String):ColArrow = {
	require(dom.table == cod.table)
	val table:Table = dom.table
  	apply(table, dom, cod, table(domcolname), table(codcolname))
  }
						  
}

case class ColArrow(table:Table, dom:ColObject, cod:ColObject,
						domcol :Column, codcol :Column) extends Arrow{
  def name = domcol.name ++ " -> " ++ codcol.name
  def unary_~ : Arrow = ColArrow(table,cod,dom, codcol, domcol)
}

case class Tuple(arrows: List[Arrow]) extends Arrow {
  def unary_~ = {assert(false);null}
  def dom = arrows.head.dom
  def cod = arrows.head.cod
}
object Tuple{
  def apply(arrows: Arrow*) = new Tuple(arrows.toList)
}
case class NamedTuple(arrows: List[(String,Arrow)]) extends Arrow {
  def unary_~ = {assert(false);null}
  def dom = arrows.head._2.dom
  def cod = throw new Exception("not implemented")
}

object NamedTuple{
  def apply(arrows: (String,Arrow)*) = new NamedTuple(arrows.toList)
}

case class Composition(left:Arrow,right:Arrow) extends Arrow {
  def dom = left.dom
  def cod = right.cod
  override def toString:String = "(%s ++ %s)" format (left,right)
  def unary_~ : Arrow = (~right)>>>(~left)
  require (
  	left.cod == right.dom || right.dom == UnitObject,
  	"A >>> B, A.cod must be equal to B.dom but %s != %s"
  	format (left.cod , right.dom))
}

object Composition{
  def gen(left:Arrow,right:Arrow):Arrow = if (right.isIdentity) left else
	left match {
	  case _ : Identity => right
      case Composition(ll,lr) => gen(ll, gen(lr,right))
      case _ => new Composition(left,right)
	}
}
