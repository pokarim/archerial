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


```scala
val syainTable = Table("syain", List(
  Column("id", int.primaryKey.autoIncrement),
  Column("name", varchar(200)),
  Column("boss_id", int)
))

```
