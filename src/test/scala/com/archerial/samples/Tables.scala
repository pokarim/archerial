package com.archerial.samples
import com.archerial._

object Tables extends Metadata{
  val syain = Table("syain", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("name", ColType.varchar(200)),
    Column("age", ColType.int),
    Column("dept_id", ColType.int),
    Column("superior_id", ColType.int)
  ))
  val syainInfo = Table("syainInfo", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("info", ColType.varchar(200))
  ))
  
  val hobby = Table("hobby", List(
    Column("id", ColType.int.primaryKey.autoIncrement),
    Column("syain_id", ColType.int),
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
  def all = List(syain,syainInfo,hobby,dept,area)
}
