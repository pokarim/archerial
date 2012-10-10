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

package com.archerial.utils
import scala.collection.mutable

object SetUtil {
  def intersectAll[A](sets:Seq[Set[A]]) = {
	if (sets.isEmpty) Set[A]() 
	else if (sets.length == 1) sets(0)
	else sets.reduceLeft(_ & _)
  }
}

trait Rel[A,B]{
  def |[A2 >: A, B2 >: B](right:Rel[A2,B2]):Rel[A2,B2]

  def ++[B2 >: B,C](right:Rel[B2,C]):Rel[A,C]
 def apply(a:A):Seq[B]
  def one(a:A):B = {val xs = apply(a)
					assert(xs.nonEmpty,"Rel xs.nonEmpty")
					assert(xs.tail.isEmpty,"Rel")
					xs.head}
  def getOne(a:A):Option[B] = {val xs = apply(a)
					if (xs.nonEmpty)
					  None
					else if (xs.tail.isEmpty)
					  None 
					else xs.headOption}
  def inverse:Rel[B,A]
  def pairs:Seq[(A,B)]
  def lefts:Seq[A] = pairs.map(_._1).distinct
  def rights:Seq[B] = pairs.map(_._2).distinct
}
case class EndoMapRel[A](pairs:Seq[(A,A)]) extends Rel[A,A] with BinDefs[A,A]{
  def asTree(root:A):TreeRel[A] = TreeRel(pairs,root)
}

trait TreeRel[A] extends Rel[A,A] {
  def root:A
}
object TreeRel{
  def apply[A](pairs:Seq[(A,A)],root:A) = TreeRelByPairs(pairs,root)
}
case class TreeRelByPairs[A](pairs:Seq[(A,A)],root:A) extends TreeRel[A] with BinDefs[A,A] {
}
object Rel{
  implicit def toEndoMap[A](b:Rel[A,A]) = 
	EndoMapRel[A](b.pairs)
  def gen[A,B](xs:Seq[A])(f:A => Seq[B]) = apply(
	for (x <- xs; y <- f(x)) yield (x,y))

 def apply[A,B](xs:Seq[(A,B)]):Rel[A,B] = RelByPairs[A,B](xs)
  def toU[A,A2 >: A,B,B2 >: B](x:Rel[A,B]):Rel[A2,B2] = x match {
	case RelByPairs(xs) => Rel[A2,B2](xs) 
  }
	
}

trait BinDefs[A,B] {
  def pairs:Seq[(A,B)]
  def ++[B2 >: B,C](right:Rel[B2,C]):Rel[A,C] = 
  	RelByPairs(for {(a,b) <- pairs;
					   c <- right(b)} yield (a,c))

  def |[A2 >: A, B2 >: B](right:Rel[A2,B2]):Rel[A2,B2] = right match {
  	case RelByPairs(ys) => Rel(pairs ++ ys) 
  }
  def apply(a:A):Seq[B] = a2b.getOrElse(a,Nil)
  lazy val a2b = SeqUtil.groupTuples(pairs.distinct)
  lazy val inverse:Rel[B,A] = Rel[B,A](pairs.map(_.swap))
}
case class RelByPairs[A,B](pairs:Seq[(A,B)]) extends Rel[A,B] with BinDefs[A,B]{
}

object GraphUtil {
  import collection.mutable
  def topologicalSort[Node](nodes:Iterable[Node])(parents: Node=>Iterable[Node]):Seq[Node] = {
	val buf = Seq.newBuilder[Node]
	val visited = mutable.HashSet[Node]()
	def visit(n:Node):Unit =
	  if (!visited(n)) {
		visited += n
		parents(n).foreach(visit(_))
		buf += n
	  }
	nodes.foreach(visit(_))
	buf.result
  }
}

object prn{
  def apply(xs : Any*){
    for (x <- xs) {print(x); print(" ")}
    println("")
  }
}
class Dict[A,B](val self: mutable.Map[A,B]){
  def setdefault(key:A, default: => B) = 
    if (self.contains(key)) self(key) else {
      val value = default
      self(key) = value
      value
    }
}

object NatType extends Enumeration {
  val GtOne = Value
  val One = Value
  val Zero = Value
  def toNatType(x:Int) = x match {
	case 0 => Zero
	case 1 => One
	case _ if x > 1 => GtOne
  }
}

object Dict{
  implicit def map2dict[A,B](m: mutable.Map[A,B]):Dict[A,B] =
    new Dict(m)
}
class PySeq(xs:Seq[Any]){
  def joinWith(s:String)=if(xs.isEmpty) "" else xs.map(_.toString).reduceLeft(_ ++ s ++ _)
}
object implicits{
  implicit def toPySeq(xs:Seq[Any]):PySeq = new PySeq(xs)
  implicit def map2dict[A,B](m: mutable.Map[A,B]):Dict[A,B] =
    new Dict(m)
}
import scala.collection.GenTraversableOnce

object toNestedMap{

  import scala.collection.immutable
  import scala.collection.mutable
  def apply[A,B,C](xs: GenTraversableOnce[(A,B,C)]):immutable.Map[A,Map[B,C]] ={
	import com.archerial.utils.implicits._
    val m = mutable.Map[A,mutable.Map[B,C]]()
    for ((a,b,c) <- xs.toSeq) m.setdefault(a, mutable.Map[B,C]())(b) = c
    (for ((k,vs) <- m) yield (k,vs.toMap)).toMap
  }
    
}
case class Tree[A](node:A, children:List[Tree[A]]=Nil){
  
}
object Tree{
  import SeqUtil._

  def dropTrunk[A](xs: Seq[List[A]],dropped:List[A]=Nil): (List[Tree[A]],List[A]) = {
	val ys = xs.filter(_.nonEmpty)
	if (ys.isEmpty) (Nil,dropped)
	else {
	  val head2tails = groupTuples(
		for {head::tails <- ys}
		yield head -> tails)
	  val (head, tails) = head2tails.head
	  if (head2tails.tail.isEmpty && tails.forall(_.nonEmpty)){
		dropTrunk(tails.toList, head :: dropped)
	  }
	  else
		(for {(key, tails) <- head2tails}
		 yield Tree[A](key,bundle(tails))).toList ->
	  dropped
	}
  }
  def bundle[A](xs: Seq[List[A]]): List[Tree[A]] = {
	val ys = xs.filter(_.nonEmpty)
	if (ys.isEmpty) Nil
	else {
	  val head2tails = groupTuples(
		for {head::tails <- ys}
		yield head -> tails)
	  (for {(key, tails) <- head2tails}
	   yield Tree[A](key,bundle(tails))).toList
	}
  }
  
}

object SeqUtil {
  def tailOption[A,B[_] <: Seq[_]](xs:B[A]) =
	if(xs.isEmpty) None
	else Some(xs.tail)

  def allAreSame[A](xs:Seq[A]):Boolean =
	xs.isEmpty || !xs.tail.exists(xs.head != _)

  def getOne[A](xs:Seq[A]):A = {
	assert(xs.tail.isEmpty,"getOne")
	xs.head
  }
  def transpose[A](xs: List[List[A]]): List[List[A]] = {
	if (xs.nonEmpty && xs.forall(_.nonEmpty))
	  xs.map{ _.head } :: transpose(xs.map{ _.tail })
  else Nil
  }

  import scala.collection.SeqLike
  def distinctBy1stAndGet2nd[Key,Val](self:Seq[(Key,Val)]):Seq[Val] = {
    val b = Seq.newBuilder[Val]
    val seen = mutable.HashSet[Key]()
    for (x <- self) {
	  val key = x._1
      if (!seen(key)) {
        b += x._2
        seen += key
      }
    }
    val result = b.result
	result
	
  }
  def distinct[A,Key](self:Seq[A])(f : A => Key): Seq[A] = {
    val b = Seq.newBuilder[A]
    val seen = mutable.HashSet[Key]()
    for (x <- self) {
	  val key = f(x)
      if (!seen(key)) {
        b += x
        seen += key
      }
    }
    b.result
  }

  import scala.collection.immutable

  def groupTuples[K,V](xs:Seq[(K,V)]):immutable.Map[K,Seq[V]] = {
	import scala.collection.mutable.ArrayBuffer
	import scala.collection.mutable.Map
	val map = Map[K,ArrayBuffer[V]]()
	import com.archerial.utils.implicits._
	for ((k,v) <- xs){
	  map.setdefault(k, new ArrayBuffer[V]).append(v)
	}
	(for((k,v)<-map) yield (k,v.toSeq)).toMap
  }
  def groupBy[X,K,V](xs:Seq[X])(key:X => Option[K], value: X => Option[V]) = {
	import scala.collection.mutable.ArrayBuffer
	import scala.collection.mutable.Map
	import com.archerial.utils.implicits._
	val map = Map[K,ArrayBuffer[V]]()
	for (x <- xs;
		 Some(k) <- List(key(x));
		 Some(v) <- List(value(x))
	   ){
		   map.setdefault(k, new ArrayBuffer[V]).append(v)
	}
	(for ((k, v) <- map.toSeq)
	  yield (k, v.toList)).toList
  }
 
  def groupBy[X,K](key:X => Option[K], xs:Seq[X]) = {
	import scala.collection.mutable.ArrayBuffer
	import scala.collection.mutable.Map
	import com.archerial.utils.implicits._
	val map = Map[K,ArrayBuffer[X]]()
	for (x <- xs;
		 Some(k) <- List(key(x))
	   ){
		   map.setdefault(k, new ArrayBuffer[X]).append(x)
	}
	(for ((k, v) <- map.toSeq)
	  yield (k, v.toList)).toList
  }
}

case class LiftSt[A,B,S](seq: Seq[A]){
  import scalaz.{Value => _, _}
  import Scalaz._
  import StateUtil.reduceStates

  def map(f : A => State[S,B]) = {
	for {q <- init[S]}
	yield reduceStates[S,B](for (x <- seq) yield f(x))(q)
  }
}

object StateUtil{
  import scalaz.{Value => _, _}
  import Scalaz._

  def return_[S,A](x:A) = init[S].map((_) => x)
  def mapS[S,A,B](f:A => State[S,B],xs:Seq[A]):State[S,List[B]] = 
  	reduceStates(xs.map(f))

  def forS[S,A,B](xs:Seq[A])(f:A => State[S,B]):State[S,List[B]] = 
  	reduceStates(xs.map(f))
  
  def reduceStates[S,A](xs: Seq[State[S,A]]):State[S,List[A]] = {
	xs.reverse.foldLeft(
	  init[S].map(s => Nil:List[A])
	)((ys:State[S,List[A]], xs:State[S,A] ) =>  
	  for (x <- xs; y:List[A] <- ys) yield x :: y)
  }
}
