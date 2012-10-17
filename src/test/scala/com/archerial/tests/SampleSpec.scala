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


import com.archerial.exts.ToJson._
import com.codahale.jerkson.AST._//JValue
import com.codahale.jerkson.Json.{generate,parse}
import com.codahale.jerkson.Json

import com.fasterxml.jackson.databind.{MappingJsonFactory, ObjectMapper}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser => JacksonParser}



class SampleSpec extends Specification {

  "Sample 1" should {
    val h2driver = Class.forName("org.h2.Driver")
    implicit val con:java.sql.Connection = DriverManager.getConnection("jdbc:h2:mem:sampespec", "", "")
    val syainTable = Table("syain", List(
      Column("id", int.primaryKey),
      Column("name", varchar(200)),
      Column("boss_id", int)
    ))
    syainTable.createTable()
    syainTable.insertRows(
      List("id"-> 1, "name"-> "hokari", "boss_id" -> Null),
      List("id"-> 2, "name"-> "mikio", "boss_id" -> 1),
      List("id"-> 3, "name"-> "keiko", "boss_id" -> 2),
      List("id"-> 4, "name"-> "manabu", "boss_id" -> 1)
    )

    val Id = ColObject(syainTable,"id")
    val Name = ColObject(syainTable,"name")
    val name = ColArrow(Id, Name)
    val boss = ColArrow(Id, Id, "id", "boss_id")
	val sub = ~boss
    val syains = AllOf(Id)
    
    val exp = (syains >>> Filter(boss >>> name =:= "hokari") >>> name).queryExp
    pprn(exp)
    pprn(QueryExpTools.getQueryExps(Left(exp)))
    //exp.eval()
    syains.eval().prettyJsonString ===
      """[ 1, 3, 2, 4 ]"""

    {syains >>> name}.eval().prettyJsonString ===
      """[ "hokari", "keiko", "mikio", "manabu" ]"""

    {syains >>> Filter(name =:= "hokari") >>> name
           }.eval().prettyJsonString ===
             """[ "hokari" ]"""

    {syains >>> Filter(boss >>> name =:= "hokari") >>> name
           }.eval().prettyJsonString ===
             """[ "mikio", "manabu" ]"""

    {syains >>> boss }.eval().prettyJsonString ===
      """[ 2, 1, 1 ]"""
    
    {syains >>> boss >>> name}.eval().prettyJsonString ===
      """[ "hokari", "hokari", "mikio" ]"""

    {syains >>> NamedTuple("Name" ->name)}.eval().prettyJsonString ===
      """[ {
  "__id__" : [ 1 ],
  "Name" : [ "hokari" ]
}, {
  "__id__" : [ 3 ],
  "Name" : [ "keiko" ]
}, {
  "__id__" : [ 2 ],
  "Name" : [ "mikio" ]
}, {
  "__id__" : [ 4 ],
  "Name" : [ "manabu" ]
} ]"""

    (syains >>> Filter(name =:= "hokari") >>>
     NamedTuple("Name" -> name,
                "Boss" -> (boss >>> name),
                "Subordinates" -> (~boss >>> name)
              )).eval().prettyJsonString ===
                """[ {
  "__id__" : [ 1 ],
  "Name" : [ "hokari" ],
  "Boss" : [ ],
  "Subordinates" : [ "mikio", "manabu" ]
} ]"""

    {syains >>> Filter(name =:= Const("hokari")) >>>
             NamedTuple("Name" -> name,
                        "Boss" -> (boss >>> name),
                        "Subordinates" -> 
                        (~boss >>> NamedTuple("Name" -> name))
                      )
   }.eval().prettyJsonString ===
                        """[ {
  "__id__" : [ 1 ],
  "Name" : [ "hokari" ],
  "Boss" : [ ],
  "Subordinates" : [ {
    "__id__" : [ 2 ],
    "Name" : [ "mikio" ]
  }, {
    "__id__" : [ 4 ],
    "Name" : [ "manabu" ]
  } ]
} ]"""
	
	{syains >>> 
	Filter(Any(sub >>> name  =:= Const(Str("manabu"))))>>>
	NamedTuple(
	  "Name" -> name,
	  "Subordinates" -> (sub >>> name))}.eval().prettyJsonString === """[ {
  "__id__" : [ 1 ],
  "Name" : [ "hokari" ],
  "Subordinates" : [ "mikio", "manabu" ]
} ]"""

  }
}
