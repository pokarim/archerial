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
  <tr><th>id </th><th>name </th><th>boss_id </th></tr>
  <tr><td>1 </td><td>hokari </td><td>null </td></tr>
  <tr><td>2 </td><td>mikio </td><td>1 </td></tr>
  <tr><td>3 </td><td>keiko </td><td>2 </td></tr>
</table>

まずはテーブルとレコードを用意します。

```scala
    implicit val conn :java.sql.Connection = ...
    val syainTable = Table("syain", List(
      Column("id", int.primaryKey),
      Column("name", varchar(200)),
      Column("boss_id", int)
    ))
    syainTable.createTable()
    syainTable.insertRows(
      List("id"-> 1, "name"-> "hokari", "boss_id" -> Null),
      List("id"-> 2, "name"-> "mikio", "boss_id" -> 1),
      List("id"-> 3, "name"-> "keiko", "boss_id" -> 2),
      List("id"-> 4, "name"-> "manabu", "boss_id" -> 1))
```

つぎはマッピングです。
まずカラムをColObjectにマッピングします。
ColObjectは１カラムだけのテーブルのようなものです。
RDBの定義域（ドメイン）にも近いものです。

```scala
    val Id = ColObject(syainTable,"id")
    val Name = ColObject(syainTable,"name")
```

Objectの次は,カラムのペアをArrowにマッピングします。
ColArrowは、２カラムだけのテーブルのようなものです。
ColArrowは、入力元の定義域となるColObjectと
出力先の定義域となるColObjectを持ちます。
典型的な場合、その２つのオブジェクトを指定するだけでマッピングは完了です。

```scala
    val name = ColArrow(Id, Name) // Id -> Name
```

ここで定義したものはsyainテーブルのidカラムとnameカラムの２列のみからなるテーブルのようなものです。
次のように、自己結合外部キーと関連して、入力元と出力先のオブジェクトが
同一となる場合は、各カラム名も指定します。

```scala
    val boss = ColArrow(Id, Id, syainTable, "id", "boss_id") // Id -> Id
```
最後に、入力元としてUnitを取り、出力先として
Idを取るArrowを定義します。これも、２列のテーブルのようなものですが、
１列目の値にはUnitしかとらないので、実質意味を持つのは２列目だけであり、
１列のみのテーブル、単なる集合のようなものとなります。

```scala
    val syains = AllOf(Id) // Unit -> Id
```

```javascript
    syains.eval().prettyJsonString ===
      """[ 1, 3, 2, 4 ]"""

    {syains >>> name}.eval().prettyJsonString ===
      """[ "hokari", "keiko", "mikio", "manabu" ]"""

    {syains >>> Filter(name =:= "hokari") >>> name
           }.eval().prettyJsonString ===
             """[ "hokari" ]"""

    {syains >>> Filter(boss >>> name =:= "hokari") >>> name
           }.eval().prettyJsonString ===
             """[ "mikio", "manabu" ]"""

    {syains >>> boss }.eval().prettyJsonString ===
      """[ 2, 1, 1 ]"""
    
    {syains >>> boss >>> name}.eval().prettyJsonString ===
      """[ "hokari", "hokari", "mikio" ]"""

    {syains >>> NamedTuple("Name" ->name)}.eval().prettyJsonString ===
      """[ {
  "__id__" : [ 1 ],
  "Name" : [ "hokari" ]
}, {
  "__id__" : [ 3 ],
  "Name" : [ "keiko" ]
}, {
  "__id__" : [ 2 ],
  "Name" : [ "mikio" ]
}, {
  "__id__" : [ 4 ],
  "Name" : [ "manabu" ]
} ]"""

    (syains >>> Filter(name =:= "hokari") >>>
     NamedTuple("Name" -> name,
                "Boss" -> (boss >>> name),
                "Subordinates" -> (~boss >>> name)
              )).eval().prettyJsonString ===
                """[ {
  "__id__" : [ 1 ],
  "Name" : [ "hokari" ],
  "Boss" : [ ],
  "Subordinates" : [ "mikio", "manabu" ]
} ]"""

    {syains >>> Filter(name =:= Const("hokari")) >>>
             NamedTuple("Name" -> name,
                        "Boss" -> (boss >>> name),
                        "Subordinates" -> 
                        (~boss >>> NamedTuple("Name" -> name))
                      )
   }.eval().prettyJsonString ===
                        """[ {
  "__id__" : [ 1 ],
  "Name" : [ "hokari" ],
  "Boss" : [ ],
  "Subordinates" : [ {
    "__id__" : [ 2 ],
    "Name" : [ "mikio" ]
  }, {
    "__id__" : [ 4 ],
    "Name" : [ "manabu" ]
  } ]
} ]"""


	{syains >>> 
	Filter(Any(sub >>> name  =:= Const(Str("manabu"))))>>>
	NamedTuple(
	  "Name" -> name,
	  "Subordinates" -> (sub >>> name))}.eval().prettyJsonString === """[ {
  "__id__" : [ 1 ],
  "Name" : [ "hokari" ],
  "Subordinates" : [ "mikio", "manabu" ]
} ]"""


```
