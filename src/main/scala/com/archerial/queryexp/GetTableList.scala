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

object QueryExpTools{
  def isConst:QueryExp =>Boolean = _.isInstanceOf[ConstantQueryExp]
  def getParameterExps(xs:Either[Seq[QueryExp],Seq[TableExp]] )=
	getContainExps(xs).filter(_.isInstanceOf[ConstantQueryExp]).distinct

  def getContainExps:Either[Seq[QueryExp],Seq[TableExp]] => Seq[QueryExp] = 
	{case Left(qs) => qs.flatMap((x)=>getQueryExps(Left(x)))
	 case Right(ts) => ts.flatMap((x)=>getQueryExps(Right(x)))}
  def getQueryExps(self:Either[QueryExp,TableExp]):List[QueryExp] = 
	self.fold(List(_),(_)=>Nil) ++
  directParents(self).flatMap(getQueryExps)

  def getQueryExpsOM(self:Either[QueryExp,TableExp]):List[QueryExp] = 
	self.fold(List(_),(_)=>Nil) ++
  directParentsOM(self).flatMap(getQueryExpsOM)

  def getTableExps(self:QueryExp):List[TableExp] = 
	getTableExps(Left(self)).reverse.distinct

  def getTableExps(self:TableExp):List[TableExp] = 
	getTableExps(Right(self)).reverse.distinct

  def getTableExps(self:Either[QueryExp,TableExp]):List[TableExp] = 
	self.fold((_)=>Nil,List(_)) ++
  directParents(self).flatMap((x)=>getTableExps(x).distinct)

  def colNodeList(self:QueryExp):Seq[ColNode] = 
	getQueryExps(Left(self)).flatMap(directColNodes)

  def colNodeListOM(self:QueryExp):Seq[ColNode] = 
	getQueryExpsOM(Left(self)).flatMap(directColNodes)

  def colNodeList(self:TableExp):List[ColNode] =
	for {Left(cexp) <- directParents(Right(self))
		 c <- colNodeList(cexp)} yield c

  def directColNodes(self:QueryExp):List[ColNode] = self match {
	case Col(colNode) => List(colNode)
	case _ => Nil
  }

  def directParents(self:Either[QueryExp,TableExp]):List[Either[QueryExp,TableExp]] = self match{
	case Left(BinOp(left,right)) => List(Left(left),Left(right))
	case Left(NTuple(exps)) => exps.map(Left(_))
	case Left(n@NamedTupleQExp(key,exps)) => 
	  Left(key) :: n.valExps.map(Left(_))
	case Right(WhereNode(tableNode,cond)) => 
	  List(Left(cond),Right(tableNode))
	case Left(ConstCol(ConstantColExp(t,value))) => 
	  List(Left(Col(t.pk)),Right(t))
	case Right(JoinNode(_,leftcol,_)) => List(Left(leftcol))
	case Left(Col(colNode)) => List(Right(colNode.table))
	case _ => Nil
  }	

  def directParentsOM(self:Either[QueryExp,TableExp]):List[Either[QueryExp,TableExp]] = self match{
	case Left(BinOp(left,right)) => List(Left(left),Left(right))
	case Left(NTuple(exps)) => exps.map(Left(_))
	case Left(n@NamedTupleQExp(key,exps)) => 
	  Left(key) :: n.valExps.map(Left(_))
	case Right(WhereNode(tableNode,cond)) => 
	  List(Right(tableNode))
	case Left(ConstCol(ConstantColExp(t,value))) => 
	  List(Right(t))
	case Right(JoinNode(_,leftcol,_)) => List(Right(leftcol.table))
	case Left(Col(colNode)) => List(Right(colNode.table))
	case _ => Nil
  }	

}
