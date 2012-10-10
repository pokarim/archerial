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

trait Object {
  lazy val id = IdentityArrow(this)
  def columnOption:Option[Column] = None
}
object ColObject{
  def apply(table:Table,columnName:String):ColObject = 
	ColObject(table,table(columnName))

  def apply(columnName:String)(implicit table:Table):ColObject =
	ColObject(table,table(columnName))

}

case class ColObject(table :Table, column:Column ) extends Object{
  override lazy val id = IdentityArrow(this)
  override def columnOption:Option[Column] = Some(column)
  def name = column.name
  def getColNode() = ColNode(TableNode(table), column)
}

object BoolObject extends Object
object IntObject extends Object
object StrObject extends Object

case class TupleObject( objects:Seq[Object] ) extends Object
object UnitObject extends Object
