package com.archerial.samples
import com.archerial._
 



object SampleData{
  def createTables(implicit con:java.sql.Connection) {
	//import RootCat._
	Tables.create_all()
  }
  def insertSampleData(implicit con:java.sql.Connection) {
	import anorm._ 
	import com.archerial._
	//import RootCat._
	import RawValImplicits._
	Tables.syain.insertRows(
	  List("age"-> 15, "name"-> "hokari", "dept_id" -> 1, "boss_id" -> 1),
	  List("age"-> 16, "name"-> "mikio", "dept_id" -> 1, "boss_id" -> 1),
	  List("age"-> 17, "name"-> "keiko", "dept_id" -> 2, "boss_id" -> 2))
	Tables.dept.insertRows(
	  List("name"-> "kaihatu", "area_id" -> 1 ),
	  List("name"-> "kenkyu",  "area_id" -> 1))
	Tables.area.insertRows(List("name"-> "Tokyo"))
	Tables.hobby.insertRows(
	  List("syain_id"-> 1, "name"-> "piano", "rank" -> "A" ),
	  List("syain_id"-> 1, "name"-> "piano", "rank" -> "B" ),
	  List("syain_id"-> 1, "name"-> "tennis", "rank" -> "A"),
	  List("syain_id"-> 3, "name"-> "piano", "rank" -> "A")
	)

	Tables.syainInfo.insertRows(
	  List("info"-> "hokariInfo")
	)
	
  }  
}
