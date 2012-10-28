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

package com.archerial.tests

import org.specs2.mutable._ 

import com.archerial._
import com.archerial.arrows._
import com.archerial.objects._
import com.archerial.queryexp._
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._

import com.archerial.samples.{SampleData,Tables,Mappers}
import com.archerial.samples.Mappers._
import java.sql.{ Array => _, _ }
import javax.sql._
import ValueImpilcits._
import RawVal._
import RawValImplicits._
import ColType._
import com.codahale.jerkson.Json._
import com.codahale.jerkson.AST._//JValue
import play.api.libs.json.JsValue

import ValueImpilcits._
import RawVal._
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._

import com.archerial.exts.ToJson._
class JerksonSpec2 extends Specification {

  "Sample 1" should {
    "simple arrow" in {
	  val value = 
		List(VTuple(VList(Int(1)),
					NamedVTuple("id"->Int(1)),
					VList(Str("hokari"))))
	  pprn(value)
	  val jvalue = toJValue(value(0))
	  pprn(generate(jvalue))
	  pprn(generate(value:JValue))
	  pprn(generate(JInt(15)))
	  0===0
	}
  }

}


/*
sealed trait JsValue {...}
case object JsNull extends JsValue {...}
case class JsUndefined(error: String) extends JsValue {...}
case class JsBoolean(override val value: Boolean) extends JsValue
case class JsNumber(override val value: BigDecimal) extends JsValue
case class JsString(override val value: String) extends JsValue
case class JsArray(override val value: List[JsValue]) extends JsValue {...}
case class JsObject(override val value: Map[String, JsValue]) extends JsValue  {...}
*/
