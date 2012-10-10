package com.archerial.samples
import com.archerial._

object Mappers{
  import Tables._
  
  class forTableOf(table: Table){
	implicit val table_ :Table = table
  }
  
  object TableObjects {
    val SyainId = ColObject(syain,"id")
    val SyainInfoId = SyainId
    val HobbyId = ColObject(hobby,"id")
    val DeptId = ColObject(dept,"id")
  }
  import TableObjects._
  object ForeginArrows{
    val hobby2syain = ColArrow(hobby, HobbyId, SyainId, "id", "syain_id")
	
    val syain2dept = ColArrow(syain, SyainId, DeptId, "id", "dept_id")
	
    val syain2boss = ColArrow(syain,SyainId, SyainId, "id", "boss_id")
    val syain2subordinates = ~syain2boss
  }
  import ForeginArrows._
  object Dept extends forTableOf(Tables.dept){
    val Id = DeptId
    val Name = ColObject("name")
    val syains = ~syain2dept
  }
  
  object Syain extends forTableOf(Tables.syain){
    import Tables.syainInfo
    val Id = SyainId
	
    val Name = ColObject("name")
    val Age = ColObject("age")
    val name = ColArrow(Id, Name, "id","name")
    val id2id = ColArrow(Id, Id, "id","id")
    val age = ColArrow(Id, Age, "id","age")
    val dept = syain2dept
    val info = ColArrow(syainInfo,Id, DeptId, "id", "info")
    val hobbies = ~hobby2syain
    val boss = syain2boss
    val subordinates = syain2subordinates
  }
  object Hobby extends forTableOf(Tables.hobby){
    val Id = HobbyId
    val SyainId = TableObjects.SyainId
    val Name = ColObject("name")
    val name = ColArrow(Id, Name, "id", "name")
    val syain = hobby2syain
  }
}

