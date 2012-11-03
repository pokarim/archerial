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

package com.archerial.queryexp
import scala.collection.immutable
import com.archerial._

import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._

object QueryExpTools{
  def isConst:QueryExp =>Boolean = _.isInstanceOf[ConstantQueryExp]
  def getConsts(exps:Either[Seq[QueryExp],Seq[TableExp]]):Seq[ConstantQueryExp] =
	getContainExps(exps).flatMap(_.constants)

  def getContainExps:Either[Seq[QueryExp],Seq[TableExp]] => Seq[QueryExp] = {
	case Left(qs) => qs.flatMap((x)=>getQueryExps(Left(x)))
	case Right(ts) => ts.flatMap((x)=>getQueryExps(Right(x)))}

  def getContainTables:Either[Seq[QueryExp],Seq[TableExp]] => Seq[TableExp] = {
	case Left(qs) => qs.flatMap((x)=>getTableExps(Left(x))).reverse.distinct
	case Right(ts) => ts.flatMap((x)=>getTableExps(Right(x))).reverse.distinct}

  def getQueryExps(self:Either[QueryExp,TableExp],pot:Set[Either[QueryExp,TableExp]]=Set()):List[QueryExp] = {
	if (pot(self)) Nil
	else{
	  val pot2 = pot + self
	  self.fold(List(_),(_)=>Nil) ++
	  directParents(self).flatMap(getQueryExps(_,pot2))
	}
  }

  def getQueryExpsOM(self:Either[QueryExp,TableExp]):List[QueryExp] = {
   	self.fold(List(_),(_)=>Nil) ++
   directParentsOM(self).flatMap(getQueryExpsOM)
  }

  def getTableExps(self:QueryExp):List[TableExp] = 
	getTableExps(Left(self)).reverse.distinct

  def getTableExps(self:TableExp):List[TableExp] = 
	getTableExps(Right(self)).reverse.distinct

  def getTableExps(self:Either[QueryExp,TableExp],pot:Set[TableExp]=Set()):List[TableExp] = {
	if (self.fold((_)=>false,pot(_)))
	  Nil
	else{
	  val pot2 = self.fold((_)=>pot,(t)=> pot + t)
	  self.fold((_)=>Nil,List(_)) ++
	  directParents(self).flatMap{
		case r if r == self=> Nil
		case x => 
		  getTableExps(x,pot2).distinct}
	}
  }

  def colNodeList(self:QueryExp):Seq[ColExp] = self match {
	case self =>
	getQueryExps(Left(self)).flatMap(directColNodes)
  }

  def colNodeListOM(self:QueryExp):Seq[ColExp] = self match {
	case self =>
	getQueryExpsOM(Left(self)).flatMap(directColNodes)
  }

  def colNodeList(self:TableExp):List[ColExp] =
	 for {Left(cexp) <- directParents(Right(self))
				 c <- colNodeList(cexp)} yield c

  def directColNodes(self:QueryExp):List[ColExp] = self match {
	case Col(colNode) if !colNode.table.isGrouped
	=> List(colNode)
	case _ => Nil
  }
  def directParentTableExps(self:QueryExp) = 
	directParents(Left(self)).flatMap{
	  case Right(table) => List(table) case _ => Nil}

  def directParents(self:Either[QueryExp,TableExp]):List[Either[QueryExp,TableExp]] = self match{
	case Left(BinOp(left,right)) => List(Left(left),Left(right))
	case Left(NTuple(exps)) => exps.map(Left(_))
	case Left(AggregateFuncQExp(group,value)) => 
	  directParents(Left(value)) ++ List(Right(group))
	  //Right(group) :: directParents(Left(value))
	
	case Left(n@NamedTupleQExp(key,exps)) => 
	  Left(key) :: n.valExps.map(Left(_))

	case Right(WhereNode(tableNode,cond)) => 
	  List(Left(cond),Right(tableNode))

	case Right(GroupByNode(tableNode,key)) => 
	  List(Left(key))
//	  List(Right(tableNode))

	case Left(ConstCol(ConstantColExp(t,value))) => 
	  List(Left(Col(t.pk)),Right(t))
	case Right(j@JoinNode(_,leftcol,_,_)) => 
	  j.cond.map(Left(_)).toList ++ 
	  List(Left(leftcol))

	case Left(Col(colNode)) => 
	  List(Right(colNode.table))
	case Left(NonNullQExp(Col(colNode))) => 
	  List(Right(colNode.table))
	case _ => Nil
  }	

  def directParentsOM(self:Either[QueryExp,TableExp]):List[Either[QueryExp,TableExp]] = self match{
	case Left(BinOp(left,right)) => List(Left(left),Left(right))
	case Left(NTuple(exps)) => exps.map(Left(_))
	case Left(AggregateFuncQExp(group,value)) => 
	  Right(group) :: directParents(Left(value))
//	  directParents(Left(value)) ++ List(Right(group))
	case Left(n@NamedTupleQExp(key,exps)) => 
	  Left(key) :: n.valExps.map(Left(_))
	case Right(WhereNode(tableNode,cond)) => 
	  List(Right(tableNode))

	case Right(GroupByNode(tableNode,key)) => 
	  List(Left(key))

	case Left(ConstCol(ConstantColExp(t,value))) => 
	  List(Right(t))
	case Right(JoinNode(_,leftcol,_,_)) => List(Right(leftcol.table))
	case Left(Col(colNode)) => List(Right(colNode.table))
	case Left(NonNullQExp(Col(colNode))) => 
	  List(Right(colNode.table))
	case _ => Nil
  }	

  def getCols(x:QueryExp, key:ColExp=UnitTable.pk)
  :Stream[(ColExp,ColExp)] = 
	x match {
	  case x:Columnable => Stream(key -> x.qexpCol)
	  case Col(colNode) => Stream(key ->  colNode)
	  case NTuple(exps) =>
		for {(_,k) <- getCols(exps.head, key)
			 vexp <- exps.tail
			 x <- (key,k) #:: getCols(vexp, k)} 
		yield x
	  case NamedTupleQExp(keycol, exps) => 
		for {(_,k) <- getCols(keycol, key)
			 (_, vexp) <- exps
			 x <- (key,k) #:: getCols(vexp,k)}
		yield x
	  case _ => Stream.Empty
	}
}
