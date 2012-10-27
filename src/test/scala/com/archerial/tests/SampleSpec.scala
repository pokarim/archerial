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
    implicit val con:java.sql.Connection = DriverManager.getConnection("jdbc:h2:mem:samplespec", "", "")
    val staffTable = Table("staff", List(
      Column("id", int.primaryKey),
      Column("name", varchar(200)),
      Column("boss_id", int),
      Column("height", int)
    ))
    staffTable.createTable()
    staffTable.insertRows(
      List("id"-> 1, "name"-> "Guido", "boss_id" -> Null,
		   "height" -> 170),
      List("id"-> 2, "name"-> "Martin", "boss_id" -> 1,
		   "height" -> 160),
      List("id"-> 3, "name"-> "Larry", "boss_id" -> 2,
		   "height" -> 150),
      List("id"-> 4, "name"-> "Rich", "boss_id" -> 1,
		   "height" -> 180
		 )
    )

    val Id = ColObject(staffTable,"id")
    val Name = ColObject(staffTable,"name")
    val Height = ColObject(staffTable,"height")
    val name = ColArrow(Id, Name)
    val height = ColArrow(Id, Height)
    val boss = ColArrow(Id, Id, "id", "boss_id")
	val sub = ~boss
    val staffs = AllOf(Id)
    
    val exp = (staffs >>> Filter(boss >>> name =:= "Guido") >>> name).queryExp

    staffs.eval().prettyJsonString ===
      """[ 1, 2, 3, 4 ]"""

    {staffs >>> name}.eval().prettyJsonString ===
      """[ "Guido", "Martin", "Larry", "Rich" ]"""

    {staffs >>> Filter(name =:= "Guido") >>> name
           }.eval().prettyJsonString ===
             """[ "Guido" ]"""

    {staffs >>> Filter(boss >>> name =:= "Guido") >>> name
           }.eval().prettyJsonString ===
             """[ "Martin", "Rich" ]"""

    {staffs >>> boss }.eval().prettyJsonString ===
      """[ 1, 2, 1 ]"""
    
    {staffs >>> boss >>> name}.eval().prettyJsonString ===
      """[ "Guido", "Martin", "Guido" ]"""

    {staffs >>> NamedTuple("Name" ->name)}.eval().prettyJsonString ===
      """[ {
  "__id__" : [ 1 ],
  "Name" : [ "Guido" ]
}, {
  "__id__" : [ 2 ],
  "Name" : [ "Martin" ]
}, {
  "__id__" : [ 3 ],
  "Name" : [ "Larry" ]
}, {
  "__id__" : [ 4 ],
  "Name" : [ "Rich" ]
} ]"""

    (staffs >>> Filter(name =:= "Guido") >>>
     NamedTuple("Name" -> name,
                "Boss" -> (boss >>> name),
                "Subordinates" -> (~boss >>> name)
              )).eval().prettyJsonString ===
                """[ {
  "__id__" : [ 1 ],
  "Name" : [ "Guido" ],
  "Boss" : [ ],
  "Subordinates" : [ "Martin", "Rich" ]
} ]"""

    {staffs >>> Filter(name =:= Const("Guido")) >>>
             NamedTuple("Name" -> name,
                        "Boss" -> (boss >>> name),
                        "Subordinates" -> 
                        (~boss >>> NamedTuple("Name" -> name))
                      )
   }.eval().prettyJsonString ===
                        """[ {
  "__id__" : [ 1 ],
  "Name" : [ "Guido" ],
  "Boss" : [ ],
  "Subordinates" : [ {
    "__id__" : [ 2 ],
    "Name" : [ "Martin" ]
  }, {
    "__id__" : [ 4 ],
    "Name" : [ "Rich" ]
  } ]
} ]"""
	
	{staffs >>> 
	Filter(Any(sub >>> name  =:= Const(Str("Rich"))))>>>
	NamedTuple(
	  "Name" -> name,
	  "Subordinates" -> (sub >>> name))}.eval().prettyJsonString === """[ {
  "__id__" : [ 1 ],
  "Name" : [ "Guido" ],
  "Subordinates" : [ "Martin", "Rich" ]
} ]"""

	 Sum(staffs >>> height).eval().prettyJsonString === 
	   "[ 660 ]"

	 Sum(staffs >>> sub >>> height).eval().prettyJsonString ===
	   "[ 490 ]"



	{staffs >>> Sum(sub >>> height)}.eval().prettyJsonString ===
	   "[ 340, 150, 0, 0 ]"

	{staffs >>> sub >>> Sum(height)}.eval().prettyJsonString //===
	   "[ 340, 150, 0, 0 ]"

	{staffs >>> NamedTuple(
	  "Name" -> name,
	  "Sum" -> Sum(sub >>> height))}.eval().prettyJsonString ===
			  """[ {
  "__id__" : [ 1 ],
  "Name" : [ "Guido" ],
  "Sum" : [ 340 ]
}, {
  "__id__" : [ 2 ],
  "Name" : [ "Martin" ],
  "Sum" : [ 150 ]
}, {
  "__id__" : [ 3 ],
  "Name" : [ "Larry" ],
  "Sum" : [ 0 ]
}, {
  "__id__" : [ 4 ],
  "Name" : [ "Rich" ],
  "Sum" : [ 0 ]
} ]"""


	{staffs >>> Filter(name =:= Const("Guido")) >>>
		  NamedTuple(
			"Subordinates" -> (sub >>> NamedTuple(
			  "Height" -> height)),
			"Sum" -> Sum(sub >>> height))
   }.eval().prettyJsonString === 
	 """[ {
  "__id__" : [ 1 ],
  "Subordinates" : [ {
    "__id__" : [ 2 ],
    "Height" : [ 160 ]
  }, {
    "__id__" : [ 4 ],
    "Height" : [ 180 ]
  } ],
  "Sum" : [ 340 ]
} ]"""


	0===0

  }
}
