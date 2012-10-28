Archerial
---------
*Binary relational-relational mapping DSL for accesing databases in Scala.*

### 簡単な紹介
#### それは何ですか

Archerial は、RDBMS と仮想的なBinary relational DBをマッピングするツールです。
Object-relational mapperならぬ、Binary relational-relational mapperです。
DSLによるクエリー記述に基づいてSQLクエリーを発行しデータを取得・加工します。

#### Binary relational DBって何ですか
Binary relational DBとは、
Binary relation == 二項関係 == ２項タプルの集合
に基づくデータベースモデルです。
一方RDBは、多項関係、つまりn項のタプルの集合に基づくデータベースモデルです。

#### 特徴
##### 関数やパイプのような結合方法
BRDBの特徴は、関数合成やパイプによる結合に似たテーブル合成方法を持つ点です。
これはRDB/SQLにおけるjoinのようなテーブル結合とは大きく異なります。
合成は、基本的に[二項関係の合成](http://ja.wikipedia.org/wiki/%E9%96%A2%E4%BF%82%E3%81%AE%E5%90%88%E6%88%90) です。
（RDBMSが集合を基本としながら多重集合を扱うように、実際は重複を含むものも扱います。）
###### 宣言的な記述で木構造データを取得可能
Archerialでは入れ子になったデータを取得するクエリを、宣言的に簡潔に記述することができます。

#### 簡単な例

<table>
  <tr><th>id </th><th>name </th><th>boss_id </th><th>height </th></tr>
  <tr><td>1 </td><td>Guido </td><td>null </td><td>170 </td></tr>
  <tr><td>2 </td><td>Martin </td><td>1 </td><td>160 </td></tr>
  <tr><td>3 </td><td>Larry </td><td>2 </td><td>150 </td></tr>
  <tr><td>4 </td><td>Rich </td><td>1 </td><td>180 </td></tr>
</table>

まずはテーブルとレコードを用意します。

```scala
    implicit val conn :java.sql.Connection = ...
    val staffTable = Table("staff", List(
      Column("id", int.primaryKey),
      Column("name", varchar(200)),
      Column("boss_id", int),
      Column("height", int)
    ))
    staffTable.createTable()
    staffTable.insertRows(
      List("id"-> 1, "name"-> "Guido", "boss_id" -> Null, "height" -> 170),
      List("id"-> 2, "name"-> "Martin", "boss_id" -> 1, "height" -> 160),
      List("id"-> 3, "name"-> "Larry", "boss_id" -> 2, "height" -> 150),
      List("id"-> 4, "name"-> "Rich", "boss_id" -> 1, "height" -> 180
		 )
    )
```

つぎはマッピングです。
まずカラムをColObjectにマッピングします。
ColObjectは１カラムだけのテーブルのようなものです。
RDBの定義域（ドメイン）にも近いものです。

```scala
    val Id = ColObject(staffTable,"id")
    val Name = ColObject(staffTable,"name")
    val Height = ColObject(staffTable,"height")
```

Objectの次は,カラムのペアをArrowにマッピングします。
ColArrowは、２カラムだけのテーブルのようなものです。
ColArrowは、入力元の定義域となるColObjectと
出力先の定義域となるColObjectを持ちます。
典型的な場合、その２つのオブジェクトを指定するだけでマッピングは完了です。

```scala
    val name = ColArrow(Id, Name) // Id -> Name
    val height = ColArrow(Id, Height)
```
nameの中身は概念的には例えば次のようなデータを持ちます。

```scala
Set(1 -> "Guido",
    2 -> "Martin",
    3 -> "Larry",
    4 -> "Rich")
```

ここで定義したものはstaffテーブルのidカラムとnameカラムの２列のみからなるテーブルのようなものです。
次のように、自己結合外部キーと関連して、入力元と出力先のオブジェクトが同一となる場合は、各カラム名も指定します。

```scala
    val boss = ColArrow(Id, Id, staffTable, "id", "boss_id") // Id -> Id
```
最後に、入力元としてUnitを取り、出力先として
Idを取るArrowを定義します。これも、２列のテーブルのようなものですが、
１列目の値にはUnitしかとらないので、実質意味を持つのは２列目だけであり、
１列のみのテーブル、単なる集合のようなものとなります。

```scala
    val staffs = AllOf(Id) // Unit -> Id
```

このAllOf(Id)の中身は、概念的には例えば次のようになります。

```scala
Set(Unit -> 1,
    Unit -> 2,
    Unit -> 3,
    Unit -> 4)
```
このクエリーを実行するにはeval()メソッドを呼び出します。

```javascript
    staffs.eval().prettyJsonString ===
      """[ 1, 3, 2, 4 ]"""

```
eval()メソッドは、UnitObjectを入力元とするArrowに定義され、
関係の第二成分の多重集合を戻します。

```javascript

    {staffs >>> name}.eval().prettyJsonString ===
      """[ "Guido", "Larry", "Martin", "Rich" ]"""

    {staffs >>> Filter(name =:= "Guido") >>> name
           }.eval().prettyJsonString ===
             """[ "Guido" ]"""

    {staffs >>> Filter(boss >>> name =:= "Guido") >>> name
           }.eval().prettyJsonString ===
             """[ "Martin", "Rich" ]"""

    {staffs >>> boss }.eval().prettyJsonString ===
      """[ 2, 1, 1 ]"""
    
    {staffs >>> boss >>> name}.eval().prettyJsonString ===
      """[ "Guido", "Guido", "Martin" ]"""

    {staffs >>> NamedTuple("Name" ->name)}.eval().prettyJsonString ===
      """[ {
  "__id__" : [ 1 ],
  "Name" : [ "Guido" ]
}, {
  "__id__" : [ 3 ],
  "Name" : [ "Larry" ]
}, {
  "__id__" : [ 2 ],
  "Name" : [ "Martin" ]
}, {
  "__id__" : [ 4 ],
  "Name" : [ "Rich" ]
} ]"""

    (staffs >>> Filter(name =:= "Guido") >>>
     NamedTuple("Name" -> name,
                "Boss" -> (boss >>> name),
                "Subordinates" -> (~boss >>> name)
              )).eval().prettyJsonString ===
                """[ {
  "__id__" : [ 1 ],
  "Name" : [ "Guido" ],
  "Boss" : [ ],
  "Subordinates" : [ "Martin", "Rich" ]
} ]"""

    {staffs >>> Filter(name =:= Const("Guido")) >>>
             NamedTuple("Name" -> name,
                        "Boss" -> (boss >>> name),
                        "Subordinates" -> 
                        (~boss >>> NamedTuple("Name" -> name))
                      )
   }.eval().prettyJsonString ===
                        """[ {
  "__id__" : [ 1 ],
  "Name" : [ "Guido" ],
  "Boss" : [ ],
  "Subordinates" : [ {
    "__id__" : [ 2 ],
    "Name" : [ "Martin" ]
  }, {
    "__id__" : [ 4 ],
    "Name" : [ "Rich" ]
  } ]
} ]"""


	{staffs >>> 
	Filter(Any(sub >>> name  =:= Const(Str("Rich"))))>>>
	NamedTuple(
	  "Name" -> name,
	  "Subordinates" -> (sub >>> name))}.eval().prettyJsonString === """[ {
  "__id__" : [ 1 ],
  "Name" : [ "Guido" ],
  "Subordinates" : [ "Martin", "Rich" ]
} ]"""

```
#### キーごとの集計：SUM

Sumは例えば、次のようなペアの集合から

```scala
Set(1 -> 3,
	1 -> 5,
	2 -> 2,
	2 -> 3,
	2 -> 4)
```
次のようなペアの集合を生成します。

```scala
Set(1 -> 8, // 8 == 3 + 5
	2 -> 9) // 9 == 2 + 3 + 4

```
SQLにおける"select sum(..) .. group by ..;" に相当します。
第一成分でグループ化し、第二成分の合計を得るわけです。

各Staffについて、その部下（Subordinates）の身長(height)の合計を求めるのであれば、次のようになります。
部下の身長は
sub >>> height
であり、
Sum(sub >>> height)
は、各staffと、その部下のheightの合計の間の二項関係を表します。
これをすべてのStaffについて求める式は、
{staffs >>> Sum(sub >>> height)}
です。

```scala
	{staffs >>> 
  	 Sum(sub >>> height)}.eval().prettyJsonString ===
	   "[ 340, 150, 0, 0 ]"

```

#### 全体の集計
キーごとの合計でなく、全体を合計を得たい場合は、
ただひとつの値をもつ集合であるUnitObjectをドメインにもつ関係の合計を取ります。
staff全員のheightの合計を求めるArrowは以下のようになります。

```scala

	Sum(staffs >>> height).eval().prettyJsonString === 
	  "[ 660 ]"

```
(staffs >>> height)は次のような情報を持ちます。
```scala
Set(Unit -> 170,
	Unit -> 160,
	Unit -> 150,
	Unit -> 180)
```
Sum(staffs >>> height) の結果はつぎのようになります。

```scala
Set(Unit -> 660) //660  == 170 + 160 + 150 + 180

```

```scala

	{staffs >>> Filter(name =:= Const("Guido")) >>>
		  NamedTuple(
			"Subordinates" -> (sub >>> NamedTuple(
			  "Height" -> height)),
			"Sum" -> Sum(sub >>> height))
   }.eval().prettyJsonString === 
	 """[ {
  "__id__" : [ 1 ],
  "Subordinates" : [ {
    "__id__" : [ 2 ],
    "Height" : [ 160 ]
  }, {
    "__id__" : [ 4 ],
    "Height" : [ 180 ]
  } ],
  "Sum" : [ 340 ]
} ]"""


```
