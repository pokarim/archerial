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

import com.archerial.samples.{SampleData,Tables,Mappers}
import com.archerial.samples.Mappers._
import java.sql.{ Array => _, _ }
import javax.sql._
import ValueImpilcits._
import RawVal._
import RawValImplicits._
import ColType._
class SampeSpec extends Specification {

  "Sample 1" should {
    val h2driver = Class.forName("org.h2.Driver")
    implicit val con = DriverManager.getConnection("jdbc:h2:mem:sampespec", "", "")
    val syainTable = Table("syain", List(
      Column("id", int.primaryKey),
      Column("name", varchar(200)),
      Column("boss_id", int)
    ))
    syainTable.createTable()
    syainTable.insertRows(
      List("id"-> 1, "name"-> "hokari", "boss_id" -> 1),
      List("id"-> 2, "name"-> "mikio", "boss_id" -> 1),
      List("id"-> 3, "name"-> "keiko", "boss_id" -> 2))

    val Id = ColObject(syainTable,"id")
    val Name = ColObject(syainTable,"name")
    val name = ColArrow(Id, Name)
    val boss = ColArrow(syainTable, Id, Id, "id", "boss_id")
    val syains = AllOf(Id)
    val isMikio = name =:= Const(Str("mikio"))
    val isHokari = name =:= Const(Str("hokari"))
    
    "simple arrow" in {
      (syains >>> name).eval().toSet ===
        Set[Value](Str("hokari"),
                   Str("keiko"),
                   Str("mikio"))
    }
  }
}
