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

package com.archerial.samples
import com.archerial._

object Tables extends Metadata{
  val staff = Table("staff", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("name", ColType.varchar(200)),
    Column("sex", ColType.varchar(8)),
    Column("age", ColType.int),
    Column("dept_id", ColType.int),
    Column("boss_id", ColType.int),
    Column("height", ColType.int)
  ))
  val staffInfo = Table("staffInfo", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("info", ColType.varchar(200))
  ))

  val product = Table("product", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("name", ColType.varchar(200)),
    Column("price", ColType.int)
  ))
  
  val order = Table("orders", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("memo", ColType.varchar(200)),
    Column("staff_id", ColType.int)
  ))
  
  val orderItem = Table("orderItem", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("order_id", ColType.int),
    Column("product_id", ColType.int),
    Column("qty", ColType.int)
  ))
  
  val hobby = Table("hobby", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("staff_id", ColType.int),
    Column("name", ColType.varchar(200)),
    Column("rank", ColType.varchar(200))
  ))
  val dept = Table("dept", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("name", ColType.varchar(200)),
    Column("area_id", ColType.int)
  ))
  val area = Table("area", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("name", ColType.varchar(200))
  ))
  def all = List(staff,staffInfo,hobby,dept,area,order,product,orderItem)
}
