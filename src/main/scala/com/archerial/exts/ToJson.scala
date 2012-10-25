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

package com.archerial.exts
import com.archerial._
import com.codahale.jerkson.AST._
import com.codahale.jerkson.Json
import org.codehaus.jackson.map.{ObjectMapper,MappingJsonFactory}

object PJson extends Json {
  def prettify(x:JValue) = 
	mapper.writerWithDefaultPrettyPrinter().writeValueAsString(x)
}
trait ToJson{
  def jvalue:JValue
  def prettyJsonString:String = PJson.prettify(jvalue)
  def jsonString:String = PJson.generate(jvalue)
}
case class ValuesToJson(xs:Seq[Value]) extends ToJson{
  def jvalue = ToJson.toJValue(xs)
}

case class ValueToJson(x:Value) extends ToJson{
  def jvalue = ToJson.toJValue(x)
}

object ToJson{
  implicit def valuesToJson(xs:Seq[Value]):ValuesToJson = 
	ValuesToJson(xs)
  implicit def valuesToJson(x:Value):ValueToJson = 
	ValueToJson(x)
  
  def vseq2JArray(xs:Seq[Value]):JValue = 
	JArray(xs.map(toJValue).toList)
  implicit def toJValue(xs:Seq[Value]):JValue = 
	vseq2JArray(xs)
  implicit def toJValue(self:Value):JValue = self match {
	case VList(xs @_*) => vseq2JArray(xs)
	case VTuple(xs @_*) => vseq2JArray(xs)
	case NamedVTuple(namedValues @_*) =>
	  JObject(namedValues.map{case (s,v)=> 
		JField(s,toJValue(v))}.toList)
	case Val(RawVal.Null,_) => JNull
	case Val(RawVal.Int(x),_) => JInt(x)
	case Val(RawVal.Long(x),_) => JInt(x)
	case Val(RawVal.Str(x),_) => JString(x)
	case ErrorValue(s) => JNull
	case UnitValue => JNull
  }
  def toPrettyJsonStr(x:JValue) = PJson.prettify(x)
  def toJsonStr(x:JValue) = PJson.generate(x)
}
