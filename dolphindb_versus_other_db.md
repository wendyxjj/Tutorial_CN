# DolphinDB和传统数据库的区别

本教程是为初次使用DolphinDB的用户而准备。设计DolphinDB的初衷是为了解决海量结构化数据，尤其是时间序列数据的存储和计算。DolphinDB与传统的关系数据库相比，存在一些架构、功能、用法的不同。本教程会帮助你快速入门DolphinDB。

## 1. 创建数据库和表

在传统的关系型数据库中，创建一个数据库一般只要给定一个数据库名称即可。但是在DolphinDB中，为了解决海量数据的存储和计算，创建数据库时必须指定分区方式。数据库的分区方式包括值分区，范围分区，列表分区，哈希分区和复合分区，详细请参考[DolphinDB分区数据库教程](./database.md)。

在传统的关系型数据库中，数据库名称是数据库的唯一标识，可在系统内直接引用。DolphinDB使用一个路径而不是名称来标识一个数据库。当路径为空值、本地磁盘路径或分布式存储路径时，分别对应内存数据库，本地磁盘数据库和分布式数据库。DolphinDB之所以用一个路径而不是一个名称来标识一个数据库，简单的说，数据库和表在DolphinDB中不再是第一公民，它们与一个数组，一个集合，一个字典，甚至一个函数一样，只是一个数据对象，可以赋给某一个变量，然后在内置的编程语言和计算引擎中处理。如果我们用一个名称来标识一个数据库，那么将和编程语言中的变量名称冲突。

## 2. 访问数据表


## 3. DolphinDB编程语言


### 3.1 函数化编程

### 3.2 分布式计算

### 3.3 功能扩展

## 4. SQL

DolphinDB的SQL和传统关系数据库的SQL在基本概念和基本语法上非常接近。但在实现上有两个重大的区别，使得DolphinDB SQL能够完成更复杂的数据处理以及实现更快的运行速度。

### 4.1 彻底的向量化数据模型

传统的关系型数据库一般按行存储数据，一个表是多个行的集合，多个表之间可以方便的关联(join)，但是因为一个集合中的元素没有先后关系，行与行之间序列建模（通俗的说，先后关系建模）非常困难。为了在关系型数据库中实现序列建模，SQL标准引入了窗口函数（window function）。但窗口函数仍然有很大的局限性，可使用的计算函数非常有限。

DolphinDB的绝大部分存储引擎是列式引擎，也就是以列为单位存储数据。与列式存储配套，DolphinDB的内存计算模型也是一个彻底的向量化计算模型，向量是数据操作的基本单位。DolphinDB内置的800多个函数，大部分是向量化函数，输入是一个或多个向量，输出是标量（聚合函数）或等长的向量。当我们用向量来表示数据时，序列建模或序列操作变得非常简单和直观，无需window function这样额外的框架在关系代数中苦苦支撑序列数据建模，任何一个序列操作或模型都可以用一个新的向量化函数或若干个已知的向量化函数迭代组合来表示。

SQL标准中的window function虽然可以表示常用的序列操作，但是很难实现多个序列操作的迭代，譬如先对某一个序列做窗口为10的滑动平均操作，再和另一个序列做窗口为20的滑动beta的计算。但这样的复杂序列操作在DolphinDB中非常简单，嵌套调用多个向量化函数即可，也就是说把一个向量化函数的输出作为另一个向量化函数的输入。

面板数据(panel data)是统计或计量经济学中经常使用的多维度数据，通常涉及多个指标对时间的变化。时间序列数据和横截面数据是面板数据的特例。在面板数据分析时，我们通常需要对多个指标同时做时间序列分析，或者对多个时间点同时做横截面分析。DolphinDB引入了context by子句，用于在SQL中快捷的实现面板数据分析。

### 4.2 SQL和编程语言的彻底融合

传统的关系数据库一般用SQL语句进行查询，但使用的SQL语句通常是静态的。这里“静态”有几层含义。首先，一个SQL语句的查询结果不可以作为另一个SQL语句的输入；其次，SQL不能使用变量（数据对象）；再次，SQL和函数不能交互，譬如函数的参数、返回值、中间实现过程不能使用SQL，SQL中不能便捷的使用用户自定义函数。不少数据库提供了存储过程，这是对“静态”SQL的扩展，但是仍然有很多局限。DolphinDB中的SQL则彻底的与编程语言融合。

在DolphinDB中，一个SQL语句的查询结果可作为另一个SQL语句的输入：



SQL语句可以任意使用变量：



SQL与函数可以交互：




### 4.3 一些SQL语法的区别



## 5. 合理使用DolphinDB

传统的关系数据库或者一些NoSQL数据库因为计算功能非常有限，用户通常将其当作纯粹的存储引擎来使用。批量获取原始数据后，在Spark，Python，Matlab等计算引擎中处理数据。这种处理模式有一些局限性，包括：（1）跨系统数据传输的开销过高，（2）内存使用峰值过高，（3）不能充分利用服务器资源进行并行计算或分布式计算。这些问题都可以通过存储与计算均使用DolphinDB而得到解决。用户如果把DolphinDB仅作为存储引擎来使用，不能充分发挥DolphinDB的优势。

