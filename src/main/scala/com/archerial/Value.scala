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
import anorm.toParameterValue
import anorm.ParameterValue
import anorm._
import com.archerial.utils.implicits._
import scala.collection.SeqLike
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{Builder,ArrayBuffer}

abstract class RawVal() {
  def value:Any
  def toSQLString:String
  def toParameterValue:ParameterValue[_]
  def toShortStr:String = value.toString
  override def toString =
    "%s(%s)".format(this.getClass.getName.split("\\$")(1), value)
  final def isNull = RawVal.Null == this
}

object RawVal{
  type Type = RawVal
  object Null extends Type{
	def toSQLString = "null"
    def toParameterValue:ParameterValue[Null] = anorm.toParameterValue(null)
	override def toString = "null"
    override def toShortStr:String = toString
	def value:Null = {assert(false,"null error");null}
  }
  case class Int (value: scala.Int) extends Type {
	def toSQLString = value.toString
    def toParameterValue:ParameterValue[scala.Int] = anorm.toParameterValue(value)
  }
  case class Long (value: scala.Long) extends Type {
	def toSQLString = value.toString
    def toParameterValue:ParameterValue[scala.Long] = anorm.toParameterValue(value)
  }

  case class GeneratedId (value: scala.Long) extends Type {
	def toSQLString = "G(%s)" format value.toString
    def toParameterValue:ParameterValue[scala.Long] = anorm.toParameterValue(value)
    def next() = copy(value=value + 1)
  }
  object GeneratedId{
    val zero = GeneratedId(0)
  }

  case class Bool(value: Boolean) extends Type{
	def toSQLString = value.toString
	def toParameterValue:ParameterValue[scala.Boolean] = anorm.toParameterValue(value)
  }
  case class Str (value: String) extends Type {
    def toParameterValue:ParameterValue[String] = anorm.toParameterValue(value)
    def toSQLString = "\'%s\'" format(value)
    override def toString = "Str(\"%s\")" format(value)
  }
}

object Value{}

trait Value {
  final def isNull = this match {case Val(RawVal.Null,_) => true; case _ => false}
  final def nonNull = !isNull
  def one:VList = VList(this) 
}

case class Val(val rawval:RawVal.Type, val num:Int = 1) extends Value{
}
object RawValImplicits{
  implicit def toInt(x :Int) = RawVal.Int(x)
  implicit def toIntVal(x :Int) = Val(RawVal.Int(x))
  implicit def toStr(x :String) = RawVal.Str(x)
  implicit def toStrVal(x :String) = Val(RawVal.Str(x))
}
object ValueImpilcits{
  implicit def toValue(x:RawVal.Type):Value = Val(x,1)
}
object UnitValue extends Value {
}
case class ErrorValue(msg:String) extends Value 

object NotImplemented extends ErrorValue("Not Implemented")

case class VPair(left:Value, right:Value) extends Value


object `package` {
  type SeqValue = VSeq[Value]
}
trait VSeq[V ] extends Value with Seq[V] with SeqLike[V, VSeq[V]] 
{
  override def newBuilder: Builder[V, VSeq[V]] = VList.newBuilder.asInstanceOf[Builder[V, VSeq[V]]]
}
object VSeq{
  implicit def canBuildFrom: CanBuildFrom[Seq[Value], Value, VSeq[Value]]=
    VList.canBuildFrom
}

import scala.collection.generic.SeqFactory
object VList{
  
  def fromSeq(buf: Seq[Value]): VList = VList(buf)

  def newBuilder: Builder[Value, VList] =
    new ArrayBuffer[Value] mapResult (VList.fromSeq)

  implicit def canBuildFrom: CanBuildFrom[Seq[Value], Value, VSeq[Value]]
  = new CanBuildFrom[Seq[Value], Value, VSeq[Value]] {
    def apply(): Builder[Value, VSeq[Value]] = newBuilder
    def apply(from: Seq[Value]): Builder[Value, VSeq[Value]] = newBuilder
  }  

  def apply[X: ClassManifest](xs: Value*):VList = VList.apply(xs)
  //def apply(xs: Seq[Value]):VList = VList.apply(xs)
  // def unapplySeq(target:VList) :Option[List[Value]] = 
  //   Some(target.xs.toList)
}
case class VList(val xs: Seq[Value]) extends VSeq[Value]{
  def length = xs.length
  def apply(i:scala.Int) = xs.apply(i)
  def iterator = xs.iterator
  override def newBuilder: Builder[Value, VList] = VList.newBuilder

}

case class VTuple(xs: Value*) extends Value

case class NamedVTuple(namedValues: (String,Value)*) extends Value{
  val map = namedValues.toMap
  def apply(name:String) = map(name)
  def contains(name:String) = map.contains(name)
  def get(name:String) = map.get(name)
}

case class VStream(xs: Seq[Value]) extends Value{
  def map(f : Value => Value) = VStream(xs.map(f))
  def flatMap(f : Value => VStream) = VStream(xs.flatMap(f(_).xs))
  override def toString = "VSt(%s\n)" format (xs.toList match {
	case List() => ""
	case _ => xs.map(_.toString).reduceLeft(_ + ",\n     "+ _)
  })
}
