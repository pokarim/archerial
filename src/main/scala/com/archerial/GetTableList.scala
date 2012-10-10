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

package com.archerial
import scala.collection.immutable

object ValueExpTools{
  def getValueExps(self:Either[ValueExp,TableExp]):List[ValueExp] = 
	self.fold(List(_),(_)=>Nil) ++
  directParents(self).flatMap(getValueExps)

  def getValueExpsOM(self:Either[ValueExp,TableExp]):List[ValueExp] = 
	self.fold(List(_),(_)=>Nil) ++
  directParentsOM(self).flatMap(getValueExpsOM)

  def getTableExps(self:ValueExp):List[TableExp] = 
	getTableExps(Left(self)).reverse.distinct

  def getTableExps(self:TableExp):List[TableExp] = 
	getTableExps(Right(self)).reverse.distinct

  def getTableExps(self:Either[ValueExp,TableExp]):List[TableExp] = 
	self.fold((_)=>Nil,List(_)) ++
  directParents(self).flatMap((x)=>getTableExps(x).distinct)

  def colNodeList(self:ValueExp):Seq[ColNode] = 
	getValueExps(Left(self)).flatMap(directColNodes)

  def colNodeListOM(self:ValueExp):Seq[ColNode] = 
	getValueExpsOM(Left(self)).flatMap(directColNodes)

  def colNodeList(self:TableExp):List[ColNode] =
	for {Left(cexp) <- directParents(Right(self))
		 c <- colNodeList(cexp)} yield c

  def directColNodes(self:ValueExp):List[ColNode] = self match {
	case Col(colNode) => List(colNode)
	case _ => Nil
  }

  def directParents(self:Either[ValueExp,TableExp]):List[Either[ValueExp,TableExp]] = self match{
	case Left(BinOp(left,right)) => List(Left(left),Left(right))
	case Left(VTuple(exps)) => exps.map(Left(_))
	case Right(WhereNode(tableNode,cond)) => 
	  List(Left(cond),Right(tableNode))
	case Left(ConstCol(ConstantColExp(t,value))) => 
	  List(Left(Col(t.pk)),Right(t))
	case Right(JoinNode(_,leftcol,_)) => List(Left(leftcol))
	case Left(Col(colNode)) => List(Right(colNode.table))
	case _ => Nil
  }	

  def directParentsOM(self:Either[ValueExp,TableExp]):List[Either[ValueExp,TableExp]] = self match{
	case Left(BinOp(left,right)) => List(Left(left),Left(right))
	case Left(VTuple(exps)) => exps.map(Left(_))
	case Right(WhereNode(tableNode,cond)) => 
	  List(Right(tableNode))
	case Left(ConstCol(ConstantColExp(t,value))) => 
	  List(Right(t))
	case Right(JoinNode(_,leftcol,_)) => List(Right(leftcol.table))
	case Left(Col(colNode)) => List(Right(colNode.table))
	case _ => Nil
  }	

}
