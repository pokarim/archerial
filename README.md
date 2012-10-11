Archerial
---------
*Binary relational-relational mapping DSL for accesing databases in Scala.*

### 簡単な紹介
#### それは何ですか

Archerial は、RDBMS と仮想的なBinary relational DBをマッピングするツールです。
Object-relational mapperならぬ、Binary relational-relational mapperです。
DSLによるクエリー記述に基づいてSQLクエリーを発行しデータを取得・加工します。
（まだSQLインジェクション対策などないので、*万が一*使用の際は注意して下さい。）

#### Binary relational DBって何ですか
Binary relational DBとは、
Binary relation == 二項関係 == ２項タプルの集合
に基づくデータベースモデルです。
一方RDBは、多項関係、つまりn項のタプルの集合に基づくデータベースモデルです。

#### 特徴
##### 関数やパイプのような結合方法
BRDBの特徴は、関数合成やパイプによる結合に似たテーブル合成方法を持つ点です。
これはRDB/SQLにおけるjoinのようなテーブル結合とは大きく異なります。

###### 宣言的な記述で木構造データを取得可能
Archerialでは入れ子になったデータを取得するクエリを、宣言的に簡潔に記述することができます。

#### 簡単な例

<table>
  <tr><th>id </th><th>name </th><th>boss_id </th></tr>
  <tr><td>1 </td><td>hokari </td><td>1 </td></tr>
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

```scala
    val Id = ColObject(syainTable,"id")
    val Name = ColObject(syainTable,"name")
```

Objectの次は,カラムのペアをArrowにマッピングします。
ColArrowは、２カラムだけのテーブルのようなものです。


```scala
    val name = ColArrow(Id, Name)
    val boss = ColArrow(syainTable, Id, Id, "id", "boss_id")
    val syains = AllOf(Id)
    val isMikio = name =:= Const(Str("mikio"))
    val isHokari = name =:= Const(Str("hokari"))

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
```
