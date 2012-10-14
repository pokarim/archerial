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

import scala.collection.immutable.Map
import com.archerial._
import com.archerial.utils.implicits._

case class TableIdMap(root:TableExp, map: Map[TableExp,String], constMap: Map[ConstantQueryExp,String]) {
  def contains(x:TableExp) = !gets(x).isEmpty
  def addKeyAlias(from:TableExp,to:TableExp) = 
	copy(map=map.updated(to, map(from) ))
  def apply(x:TableExp):String = gets(x).get
  def gets(x:TableExp):Option[String] = 
  	map.get(x)

  def isRoot(t:TableExp) = t == root
  def addConstId(consts:Seq[ConstantQueryExp]) = {
	val m = {for {(k,v) <- consts.zipWithIndex}
	yield (k,TableIdMap.id2alias(v).toLowerCase)}.toMap
	copy(constMap=m)
  }
}
object TableIdMap{
  def id2alias(id:Int) = 
	if (id < ('Z'.toInt - 'A'.toInt))
	  (id + 'A'.toInt).toChar.toString 
	else "a%s" format(id)
  def genTableIdMap(tableList:List[TableExp]):TableIdMap = {
  	val seq = (for ((k,v) <- tableList.zipWithIndex)
  			   yield (k,id2alias(v))).toMap
	val constMmap = Map[ConstantQueryExp,String]()
  	new TableIdMap(tableList.head, seq,constMmap)
  }

}
