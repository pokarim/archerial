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
import com.archerial.arrows._
import com.archerial.objects._

object Mappers{
  import Tables._
  
  class forTableOf(table: Table){
	implicit val table_ :Table = table
  }
  
  object TableObjects {
    val StaffId = ColObject(staff,"id")
    val StaffInfoId = StaffId
    val HobbyId = ColObject(hobby,"id")
    val DeptId = ColObject(dept,"id")
    val OrderId = ColObject(order,"id")
    val ProductId = ColObject(product,"id")
    val OrderItemId = ColObject(orderItem,"id")
  }
  import TableObjects._
  object ForeginArrows{
    val hobby2staff = ColArrow(hobby, HobbyId, StaffId, "id", "staff_id")

    val orderItem2order = 
	  ColArrow(orderItem, OrderItemId, OrderId, "id", "order_id")
    val orderItem2product = 
	  ColArrow(orderItem, OrderItemId, ProductId, "id", "product_id")
	
    val staff2dept = ColArrow(staff, StaffId, DeptId, "id", "dept_id")
	
    val staff2boss = ColArrow(staff,StaffId, StaffId, "id", "boss_id")
    val staff2subordinates = ~staff2boss

    val order2staff = ColArrow(order,OrderId, StaffId, "id", "staff_id")

  }
  import ForeginArrows._
  object Dept extends forTableOf(Tables.dept){
    val Id = DeptId
    val Name = ColObject("name")
    val staffs = ~staff2dept
  }

  object Order extends forTableOf(Tables.order){
    val Id = OrderId
    val Memo = ColObject("memo")
    val memo = ColArrow(Id, Memo)
  	val orderItems = ~orderItem2order
	val staff = order2staff
  }

  object Product extends forTableOf(Tables.product){
    val Id = ProductId
    val Name = ColObject("name")
    val Price = ColObject("price")
    val name = ColArrow(Id, Name)
  	val orderItems = ~orderItem2product
    val price = ColArrow(Id, Price)

  }

  object OrderItem extends forTableOf(Tables.orderItem){
    val Id = OrderItemId
    val Qty = ColObject("qty")
  	val product = orderItem2product
  	val order = orderItem2order
  	val qty = ColArrow(Id,Qty)
  }

  
  object Staff extends forTableOf(Tables.staff){
    import Tables.staffInfo
    val Id = StaffId
	
    val Name = ColObject("name")
    val Sex = ColObject("sex")
    val Age = ColObject("age")
    val Height = ColObject("height")
    val name = ColArrow(Id, Name, "id","name")
    val sex = ColArrow(Id, Sex)
    val height = ColArrow(Id, Height)
    val id2id = ColArrow(Id, Id, "id","id")
    val age = ColArrow(Id, Age, "id","age")
    val dept = staff2dept
    val info = ColArrow(staffInfo,Id, DeptId, "id", "info")
    val hobbies = ~hobby2staff
    val boss = staff2boss
    val subordinates = staff2subordinates
  }
  object Hobby extends forTableOf(Tables.hobby){
    val Id = HobbyId
    val StaffId = TableObjects.StaffId
    val Name = ColObject("name")
    val name = ColArrow(Id, Name, "id", "name")
    val staff = hobby2staff
  }
}

