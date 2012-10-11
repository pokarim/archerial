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
import scala.collection.immutable.Set
import com.archerial.utils.implicits._
import com.archerial.utils._
import scalaz.{Value => _, _}, Scalaz._
import com.archerial._
import SeqUtil.groupTuples
import StateUtil.forS
import TableTree.{Tbx,TbxSet, TTree,TTrees}

case class SimpleGenTrees(table2col:Rel[TableExp,ColNode], table2children : TreeRel[Tbx]){

  import RowMulFactor.{LtOne,One,Many}

  def splitDirectChildren(root:Tbx, directChildren:Seq[Tbx]):State[TbxSet, (TTree, TTrees)] = {
	val mul2nodes = groupTuples(
	  for (t <- directChildren) yield (t.rowMulFactor,t))
	val many = mul2nodes.getOrElse(RowMulFactor.Many,Nil)
	val one = mul2nodes.getOrElse(RowMulFactor.One,Nil)
	val lt1 = mul2nodes.getOrElse(RowMulFactor.LtOne,Nil)
	for {main <- forS(many.headOption.toList ++ one)(apply(_,false))
		 other <- forS(many.drop(1) ++ lt1)(apply(_,false))}
	yield (TableTree(root,main.map(_._1).toList), 
		   main.flatMap(_._2) ++ 
		   other.map(_._1) ++ other.flatMap(_._2))
  }

  def getTTrees(root:Tbx,children:Seq[Tbx],set:TbxSet,isDirect:Boolean):State[TbxSet, (TTree, TTrees)] = {
	val getparents = table2children.inverse
	val directChildren = 
	  children.filter(!getparents(_).exists(!set(_)))
	if (directChildren.length == 1 && 1 == children.length
		&& table2col(root).isEmpty && 
		(isDirect || children.head.rowMulFactor != LtOne)
	  ){
	  for {xs <- forS(children)(apply(_, isDirect))}
	  yield (TableTree(root,xs.map(_._1).toList), xs.flatMap(_._2))
	} else splitDirectChildren(root, directChildren)

  }

  def apply(root:Tbx, isDirect:Boolean=true):State[TbxSet, (TTree, TTrees)] = {
	val children = table2children(root)
	if (children.isEmpty){
	  for {_ <- modify[TbxSet](_ + root)}
	  yield (TableTree(root,Nil),Nil)
	} else for {
	  _ <- modify[TbxSet](_ + root)
	  set <- init[TbxSet]
	  result <- getTTrees(root,children,set,isDirect)
	} yield result
  }
}

object SimpleGenTrees{

  def gen(cexp:QueryExp):List[TTree] = 
	gen(cexp.col2tableOM.inverse,cexp.table2children)

  def gen(table2col:Rel[TableExp,ColNode],table2children : TreeRel[Tbx]):List[TTree] = {
	val (main,others) = SimpleGenTrees(table2col,table2children)(table2children.root)(Set())._2
	main :: others.toList
  }

  def splitToPieces(cexp:QueryExp):List[TTree] =
	cexp.tableNodeList.map(TableTree(_,Nil)).toList

}