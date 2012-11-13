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

package com.archerial.arrows

import com.archerial.objects._
import com.archerial._
import com.archerial.queryexp._
import OpArrows._
import com.pokarim.pprinter._
import com.pokarim.pprinter.exts.ToDocImplicits._
import java.sql.Connection
import QueryExpTools.getTableExps
import scalaz.{Value => _, _}, Scalaz._

import com.archerial.utils.StateUtil.{forS,forFlatS,return_}
import RawVal.GeneratedId


case class Update(s:Stream[(ColArrow,Value,Value)],nextId:GeneratedId=GeneratedId.zero){
}

object Update{
  val empty = Update(Stream.empty)
  def append(arrow:ColArrow,id:Value,v:Value) =
	modify[Update]((u) => u.copy(s=(arrow, id, v) #:: u.s))

  def genId() =
	for {u <- init[Update]
		 _ <- modify[Update]((u) => u.copy(nextId=u.nextId.next()))
	   } yield Val(u.nextId,1)
	//modify[Update]((u) => u.copy(s=(arrow, id, v) #:: u.s))
  def updateAndAdd(arrow:ColArrow,id:Value,v:SeqValue):State[Update, Unit]=
	for {vs <- arrow.update(v)
		 _ <- append(arrow,id,vs)}
	yield ();

}
object UpdateTools{
  import Update.append
  def namedTupleUpdate(values:SeqValue,arrows:Seq[(String,Arrow)]) :State[Update, SeqValue] = {
	forFlatS(values){
	  case v:NamedVTuple
	  if v.contains("__id__") || v.contains("__new__")=> {
		val getId = 
		  if (v.contains("__id__")) {
			val VList(Seq(id)) = v("__id__")
			return_[Update,Option[Value]](Some(id))}
		  else Update.genId().map(Some(_))
		for {id <- getId
			 _ <- forS(for {(n,a) <- arrows} yield (id,a,v.get(n))){
			   case (Some(id),arrow:ColArrow, Some(vs:VList)) =>
				 append(arrow, id, vs)
			   case (Some(id),>>>(left:ColArrow, right), Some(vs:VList)) =>
				 right.update(vs) >>= {append(left, id, _)}
			   case (_,x,Some(vs:VList)) => x.update(vs)
			   case _ => {pprn("hoge");init[Update]}
			 }} yield Seq(id.getOrElse(Val(RawVal.Null,1)))
	  }
	  case v => return_(Seq(v))
	}
  }
  
}
