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

package com.pokarim.pprinter.exts

import com.pokarim.pprinter._,PPrinter._,ColorUtil.withColor
import com.archerial.queryexp._
import com.archerial.Table
import com.archerial.utils.Tree
import com.archerial._
import TableHashTool.hashCodeStr

object TableHashTool {
  def hashCodeStr(t:TableExp):DOC = {
  val h = t.hashCode.abs
   val hashCodeStr = {
   	TEXT(withColor(16 + (h / 10 % 235), "#" + ((h / 91 % 50) + 'A'.toInt).toChar.toString))
   }
	hashCodeStr
  }
}
object ToDocImplicits{
  implicit def fromSelectGen(x:SelectGen):DOC = x match {
	case SelectGen(tree, colExps, tableExps,whereCond,_,_,_) => 
	  labeledRBracketWC(
		"SelectGen",
		List[DOC](tree,tableExps(0).altRoot,colExps))
  }

  implicit def fromQueryExp(x:QueryExp):DOC = x match {

	case Exists(ctable,cond) =>
	  	  labeledRBracketWC("Exists", List[DOC](ctable,cond))
	case NonNullQExp(cond) =>
	  	  labeledRBracketWC("NonNullQExp", List[DOC](cond))
	case ConstantExp(rawVal) => 
	  	  labeledRBracketWC("NamedTupleQExp", List[DOC](rawVal))
	case c: ConstCol => 
	  	  labeledRBracketWC("ConstCol", List[DOC](
			hashCodeStr(c.constColExp.table),
			c.rawVal))
	case NTuple(exps) =>
	  labeledRBracketWC("NTuple", exps.map(x => x:DOC))
	case SumQExp(group,valcol) =>
	  labeledRBracketWC(
		"SumQExp", 
		List[DOC](hashCodeStr(group),valcol))

	case NamedTupleQExp(keycol,exps) =>
	  labeledRBracketWC(
		"NamedTupleQExp",
		(keycol :DOC) :: exps.map(x => x:DOC))
	case Col(colNode) =>
	  labeledRBracketWC("Col", List[DOC](hashCodeStr(colNode.table),colNode.column.name))
	case OpExps.=:=(left,right) =>
	  group(NEST(1, TEXT("(") <> (left:DOC) <> TEXT(" =:=") <> 
	   		LINE <> (right:DOC) <> TEXT(")")) )
	case _ => TEXT(x.toString)
  }


  implicit def fromTable(x:Table):DOC =  
	labeledRBracketWC("Table", List[DOC](x.name))


  implicit val fromTableTree = TableTreeToDOC.apply _
  implicit val fromTableExp = FromTableExp.toDOC _
  implicit val fromColExp = FromColExp.toDOC _
  implicit val fromTableIdMap = FromTableIdMap.toDOC _
  implicit val fromRow = FromRow.toDOC _
  implicit val fromValue = FromValue.toDOC _
  implicit val fromArrow = FromArrow.toDOC _
  implicit def fromTree[A <% DOC]:Tree[A] => DOC = FromTree.toDOC _
}
object FromArrow{
  import ToDocImplicits._
  import com.archerial.arrows._

  implicit def toDOC(x:Arrow):DOC = {
	x match {
	  case ColArrow(table,_,_,dcol,ccol) =>
		labeledRBracketWC(
		  "ColArrow", 
		  List[DOC](table.name,dcol,ccol))
	  case _ => x:DOC
	}
  }
}
object FromTableExp{
  import ToDocImplicits._

  implicit def toDOC(x:TableExp):DOC = {
	x match {
	  case UnitTable =>
		TEXT("UnitTable")
	  case TableNode(table,_) =>
		labeledRBracketWC("TableNode", List[DOC](hashCodeStr(x),table))
	  case JoinNode(rt,lc,rc,_) =>
		labeledRBracketWC("JoinNode", List[DOC](
		  hashCodeStr(x),lc,rt.name ++ "." ++ rc.name))

	  case CrossJoin(rt,lt) =>
		labeledRBracketWC("CrossJoin", List[DOC](
		  hashCodeStr(x),hashCodeStr(lt)))

	  case GroupByNode(t,key) =>
		labeledRBracketWC("GroupByNode", List[DOC](
		  hashCodeStr(x),hashCodeStr(t),key))
	  case WhereNode(t, cond) =>
		labeledRBracketWC("WhereNode", List[DOC](hashCodeStr(x),hashCodeStr(t),cond:DOC))
	}
  }
  
}
object TableTreeToDOC{
  import ToDocImplicits._
  def toDOCinner(x:TableTree):DOC = x match {
	case TableTree(node,xs) =>{
	  implicit val inner = toDOCinner _
	  (labeledRBracketWC2(
	  	"->",
	  	List[DOC](((node:DOC) :: xs.map((x) => x:DOC)) : _*)))
	}
  }
  def apply(x:TableTree):DOC = x match {
	case TableTree(node,xs) =>{
	  implicit val inner = toDOCinner _
	  (labeledRBracketWC2(
	  	"TableTree",
	  	List[DOC](((node:DOC) :: xs.map((x) => x:DOC)) : _*)))
	}
  }
}


object FromColExp{
  import ToDocImplicits._
  def toDOC(x:ColExp):DOC = x match {
	case ConstantColExp(table,x) =>
	  labeledRBracketWC("ConstColExp", List[DOC](hashCodeStr(table),x:DOC))
	  
	case x@ColNode(table,column) =>
	  labeledRBracketWC("ColNode", List[DOC](hashCodeStr(table),column.name))
	case x@QExpCol(exp) =>
	  labeledRBracketWC(
		"QExpCol", 
		List[DOC](exp))

  }}


object FromTableIdMap{
  implicit def toDOC(x:TableIdMap) =
	labeledRBracketWC("TableIdMap",List[DOC](x.map))
}

object FromRow{
  import ToDocImplicits.{fromRow => _,_}

  def toDOC(x:Row) =
	labeledBracketWC("Row(",x.map.toSeq.map{case (x,y) => 
										  (x:DOC,y:DOC)}, ")")
}

object FromTree{
  def toDOC[A <% DOC](x:Tree[A]):DOC = x match {
	case Tree(node,xs) =>
	  (labeledRBracketWC2(
	  	"Tree",
	  	List[DOC](((node:DOC) :: xs.map((x) => x:DOC)) : _*)))
  }
}

object FromValue{
  def showListLike(docs:Seq[DOC],start:String,end:String,rgb:(Int,Int,Int)):DOC = 
	showListLike(docs,start,end, 
				 16 + rgb._1 * 36 + rgb._2 * 6 + rgb._3 * 1)

  def showListLike(docs:Seq[DOC],start:String,end:String,color:Int=7):DOC = {
  group(
	  TEXT(withColor(color,start)) <> NEST(start.length,
										   if (docs.isEmpty) TEXT("") else
	docs.reduceLeft((x, y) => x <> TEXT(withColor(color,", ")) <> LINE <>y)
										 )
	<> TEXT(withColor(color,end))
	  )
  }
  implicit def toDOCfromRawVal(self:RawVal):DOC = self match {
	case RawVal.Str(value) => TEXT(withColor(2, "\"%s\"" ,value.toString))
	case RawVal.Null => TEXT("NULL")
	case _ => TEXT(self.toShortStr) :DOC

  }
  implicit def toDOC(self:Value):DOC = self match {
	case VPair(left, right) => toDOC(left) <> TEXT(" => ") <> NEST(
	  left.toString.length + " => ".length,toDOC(right))
	case xs: VList => 
	  showListLike(xs.xs.map(toDOC),"[","]", (4,5,5))

	case xs:VTuple => showListLike(xs.xs.map(toDOC),"{","}",5)
	case NamedVTuple(namedValues @_*) =>
	  showListLike(namedValues.map{case 
		(x,y)=>
		(x:DOC,toDOC(y)):DOC},"{","}",5)
	case Val(rawval:RawVal,num) => 
	  toDOCfromRawVal(rawval)
	case ErrorValue(s) => TEXT("ErrorValue(%s)" format s)
	case UnitValue => TEXT("UnitValue()")
  }
  
}
