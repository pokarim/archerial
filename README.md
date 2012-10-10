Archerial
=========

Archerial は、RDBMS と仮想的なBinary relational DBをマッピングするツールです。

Object-relational mapperならぬ、Binary relational-relational mapperです。

Binary relational DBとは、
Binary relation == 二項関係 == ２項タプルの集合
に基づくデータベースモデルです。
一方RDBは、多項関係、つまりn項のタプルの集合に基づくデータベースモデルです。

Binary relational DB(以下BRDB)は、RDBの各テーブルのカラムを２列に制限したものとも言えます。

BRDBの特徴は、関数合成やパイプによる結合に似たテーブル合成方法を持つ点です。
これはRDB/SQLにおけるjoinのようなテーブル結合とは大きく異なります。


id | name | boss_id
--------|------
1|Hokari|1
2|Mikio |1
3|Keiko |2