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
import scala.collection.{immutable,mutable}
import UnitTable.UnitColExp
object CC{
  val map = mutable.Map[ColExp,Int]()
  val map2 = mutable.Map[String,Int]()
}

case class Row(map : immutable.Map[ColExp,Value]=Map()){
  def contains(c:ColExp) = (c == UnitColExp || map.contains(c))
  def d(colnode:ColExp) =
	getDirectValue(colnode)
	
  def getDirectValue(colnode:ColExp) = {
	colnode match {
	  case UnitColExp => UnitValue
	  case c if contains(c) => map(c)
	  case ConstantColExp(t,value) => Val(value,1)
	  case ColNode(t,column) => {
		val target = ColNode(t.getColsRefTarget,column)
		if (contains(target))
		  map(target)
		else{
		  assert(false,("NotFound",colnode,"in",map))
		  map(target)
		}
	  }
	}
  }

  def get(colnode:ColExp):Option[Value] = map.get(colnode)
}

object Row{
  def apply(xs: Seq[(ColExp, Value)]):Row = apply(xs.toMap)
  def gen(cols:List[ColExp], data :List[Any]):Row = 
	apply(cols.zip(data).map{
	  case (x,y) => (x, x.parse2Val(y))}.toMap)

}
