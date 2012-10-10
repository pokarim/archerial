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

sealed trait Arrow extends AbstractArrow{
}

case class AllOf(cod:ColObject) extends Arrow{
  def dom = UnitObject
  def unary_~ : Arrow = 
	throw new java.lang.UnsupportedOperationException(
	  "AllOf().inverse")
}


trait EndoMap extends Arrow{
  def cod = dom
  def unary_~ : Arrow = this
}


object OpArrows{
  case class =:=(left:Arrow, right:Arrow) extends Arrow
  {
	def dom = left.dom
	def cod = BoolObject
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
	def dom = UnitObject
	def cod = getObject
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

  def apply(table:Table, dom:ColObject, cod:ColObject, domcolname :String, codcolname :String):ColArrow =
	apply(table, dom, cod, table(domcolname), table(codcolname))

  def apply(dom:ColObject, cod:ColObject, domcolname :String, codcolname :String)(implicit table:Table):ColArrow =
  	apply(table, dom, cod, table(domcolname), table(codcolname))
						  
}

case class ColArrow(table:Table, dom:ColObject, cod:ColObject,
						domcol :Column, codcol :Column) extends Arrow{
  def name = domcol.name ++ " -> " ++ codcol.name
  def unary_~ : Arrow = ColArrow(table,cod,dom, codcol, domcol)
}


case class TupleArrow(arrows: List[Arrow]) extends Arrow {

  def unary_~ = {assert(false);null}
  def dom = arrows.head.dom
  def cod = arrows.head.cod

}
object TupleArrow{
  def apply(arrows: Arrow*) = new TupleArrow(arrows.toList)
}

 
case class ComposeArrow(left:Arrow,right:Arrow) extends Arrow {
  def dom = left.dom
  def cod = right.cod
  override def toString:String = "(%s ++ %s)" format (left,right)
  def unary_~ : Arrow = (~right)>>>(~left)
  require (
  	left.cod == right.dom || right.dom == UnitObject,
  	"A >>> B, A.cod must be equal to B.dom but %s != %s"
  	format (left.cod , right.dom))
}
object ComposeArrow{
  def gen(left:Arrow,right:Arrow):Arrow = if (right.isIdentity) left else
	left match {
	  case _ : IdentityArrow => right
      case ComposeArrow(ll,lr) => gen(ll, gen(lr,right))
      case _ => new ComposeArrow(left,right)
	}
}



