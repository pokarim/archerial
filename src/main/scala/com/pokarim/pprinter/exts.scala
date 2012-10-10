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
import  com.pokarim.pprinter._,PPrinter._,ColorUtil.withColor

object TableHashTool {
  import com.archerial.TableExp
  def hashCodeStr(t:TableExp):DOC = {
  val h = t.hashCode.abs
   val hashCodeStr = {
   	TEXT(withColor(16 + (h / 10 % 235), "#" + ((h / 91 % 50) + 'A'.toInt).toChar.toString))
   }
	hashCodeStr
  }

}
import TableHashTool.hashCodeStr
object ToDocImplicits{
  import com.archerial.Select
  implicit def fromSelect(x:Select):DOC = x match {
	case Select(tree, colExps, colExps4Row, tableExps,whereCond,_) => 
	  labeledRBracketWC("Select", 
						List[DOC](tree,tableExps(0).altRoot,colExps,colExps4Row))
  }


  import com.archerial.{ValueExp,Col,VTuple,OpExps}

  implicit def fromValueExp(x:ValueExp):DOC = x match {
	case VTuple(exps) =>
	  labeledRBracketWC("VTuple", exps.map(x => x:DOC))
	case Col(colNode) =>
	  labeledRBracketWC("Col", List[DOC](hashCodeStr(colNode.table),colNode.column.name))
	case OpExps.=:=(left,right) =>
	  group(NEST(1, TEXT("(") <> (left:DOC) <> TEXT(" =:=") <> 
	   		LINE <> (right:DOC) <> TEXT(")")) )
	case _ => TEXT(x.toString)
  }


  import com.archerial.Table
  implicit def fromTable(x:Table):DOC =  
	labeledRBracketWC("Table", List[DOC](x.name))


  implicit val fromTableTree = TableTreeToDOC.apply _
  implicit val fromTableExp = FromTableExp.toDOC _
  implicit val fromColExp = FromColExp.toDOC _
  implicit val fromTableIdMap = FromTableIdMap.toDOC _
  implicit val fromRow = FromRow.toDOC _
  implicit val fromValue = FromValue.toDOC _
  import com.archerial.Tree
  implicit def fromTree[A <% DOC]:Tree[A] => DOC = FromTree.toDOC _
}

object FromTableExp{
  import com.archerial.TableExp
  import ToDocImplicits._
  implicit def toDOC(x:TableExp):DOC = {
	import com.archerial._
	x match {
	  case UnitTable =>
		TEXT("UnitTable")
	  case TableNode(table) =>
		labeledRBracketWC("TableNode", List[DOC](hashCodeStr(x),table))
	  case JoinNode(rt,lc,rc) =>
		labeledRBracketWC("JoinNode", List[DOC](
		  hashCodeStr(x),lc,rt.name ++ "." ++ rc.name))
	  case WhereNode(t, cond) =>
		labeledRBracketWC("WhereNode", List[DOC](hashCodeStr(x),hashCodeStr(t),cond:DOC))
	}
  }
  
}
object TableTreeToDOC{
  import ToDocImplicits._
  import com.archerial.TableTree
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
  import com.archerial.{ColNode,ColExp,ConstantColExp}
  def toDOC(x:ColExp):DOC = x match {
	case ConstantColExp(table,x) =>
	  labeledRBracketWC("ConstColExp", List[DOC](hashCodeStr(table),x:DOC))
	  
	case x@ColNode(table,column) =>
	  labeledRBracketWC("ColNode", List[DOC](hashCodeStr(table),column.name))

  }}


object FromTableIdMap{
  import com.archerial.TableIdMap
  implicit def toDOC(x:TableIdMap) =
	labeledRBracketWC("TableIdMap",List[DOC](x._map))
}

object FromRow{
  import com.archerial.Row
  import ToDocImplicits.{fromRow => _,_}

  def toDOC(x:Row) =
	labeledBracketWC("Row(",x.map.toSeq.map{case (x,y) => 
										  (x:DOC,y:DOC)}, ")")
}

object FromTree{
  import com.archerial.Tree
  def toDOC[A <% DOC](x:Tree[A]):DOC = x match {
	case Tree(node,xs) =>
	  (labeledRBracketWC2(
	  	"Tree",
	  	List[DOC](((node:DOC) :: xs.map((x) => x:DOC)) : _*)))
  }
}

object FromValue{
  import com.archerial._
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
	case Val(rawval:RawVal,num) => 
	  toDOCfromRawVal(rawval)
	case ErrorValue(s) => TEXT("ErrorValue(%s)" format s)
	case UnitValue => TEXT("UnitValue()")
  }
  
}
