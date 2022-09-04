# HDF5/NPY/NPY格式行情数据导入实例
DolphinDB提供了详细的[数据加载教程](./import_data.md)，以帮助用户导入数据。HDF5是一种高效的二进制数据文件格式，在数据分析领域广泛使用，DolphinDB提供了HDF5插件和[教程](https://gitee.com/dolphindb/DolphinDBPlugin/blob/master/hdf5/README_CN.md)，使用此插件可以非常方便地使用HDF5格式的数据。本文是以这两个教程为基础的一个实践案例，针对HDF5格式的行情数据导入场景，提供了一个完整的解决方案。
- [HDF5/NPY/NPY格式行情数据导入实例](#hdf5npynpy格式行情数据导入实例)
	- [1.  应用需求](#1--应用需求)
	- [2. HDF5插件](#2-hdf5插件)
		- [2. 1 插件导入](#2-1-插件导入)
		- [2. 2 常用函数](#2-2-常用函数)
	- [3. 建库建表](#3-建库建表)
		- [3.1 DolphinDB的分区机制](#31-dolphindb的分区机制)
		- [3.2 数据库设计](#32-数据库设计)
		- [3.3 建库建表脚本](#33-建库建表脚本)
	- [4. 数据导入](#4-数据导入)
		- [4.1 按天的HDF5文件导入](#41-按天的hdf5文件导入)
		- [4.2 按字段的HDF5文件导入](#42-按字段的hdf5文件导入)
		- [4.3 按字段的HDF5文件导入，并且字段列数可能不同](#43-按字段的hdf5文件导入并且字段列数可能不同)
	- [5.  NPZ数据导入](#5--npz数据导入)
## 1.  应用需求
在金融行业，HDF5数据有以下特点：
*  数据存储结构非常灵活，dataset可以随意分割，我们可以对于全部的证券按天分割，一天一个dataset，也可以按照一个证券一个dataset分割，甚至可以按照，一个证券的每个字段设置一个dataset，还可以在此基础上，有一个字段一个dataset或多个字段一个dataset。
*  相同的信息可以放在文件名，group名，dataset名或文件列内，比如按天分割时，文件名可以标识交易日期；一个证券的每个字段设置一个dataset时，dataset的名字可以是证券代码加字段，当这样表示后，数据内部就可以去除这两列，以节省存储空间。
*  不同数据提供商提供的字段也不一样，在建库建表时，我们要先搞清楚其字段内容，设置合适的字段类型，最后把需要的数据整理成合适的内存表，写入数据库。

多种多样的存储方式，不同的字段，使得读取HDF5数据的方法也是多种多样。本范例提供三种存储方式的参考，这三种数据的字段也是不同的，下面的建库建表也以此分开。
1.  一种存储方式是按天存储，所有证券的数据按天分成一个个的dataset
<br />dataset信息如下：

| tableName | tableDims | tableType |
| ------ | ------ | ------ |
| /StockTransaction/20200708 | 74802474 | H5T_COMPOUND |

<br />字段信息如下：

| name | type
| ------ | ------ |
| TradeDate | INT |
| Type | INT |
| Seq | INT |
| ExID | STRING |
| SecID | STRING |
| ExTime | INT |
| LocalTime | INT |
| TradeTime | INT |
| TradePrice | LONG |
| Volume | LONG |
| Turnover | DOUBLE |
| TradeBuyNo | LONG |
| TradeSellNo | INT |
| TradeFlag | STRING |

2、另一种存储方式是每一个证券的每一个字段分割为一个dataset，并且dataset的名字使用证券代码+字段名的方式命名，单个的文件用日期命名，数据内部不设置日期、证券代码和字段名。因此，我们需要从文件名中提出日期信息，从dataset的名字中提取证券代码和列名，和读取的数据一起生成数据表。
<br />dataset信息如下：

| tableName | tableDims | tableType |
| ------ | ------ | ------ |
| /000001.sz/AskOrder | 107663 | H5T_NATIVE_UINT |
| /000001.sz/BSFlag | 107663 | H5T_STRING |
| /000001.sz/BidOrder | 107663 | H5T_NATIVE_UINT |
| /000001.sz/BizIndex | 107663 | H5T_NATIVE_ULLONG |
| /000001.sz/Channel | 107663 | H5T_NATIVE_UINT |
| /000001.sz/FunctionCode | 107663 | H5T_STRING |
| /000001.sz/Index | 107663 | H5T_NATIVE_UINT |
| /000001.sz/OrderKind | 107663 | H5T_STRING |
| /000001.sz/Price | 107663 | H5T_NATIVE_FLOAT |
| /000001.sz/Time | 107663 | H5T_NATIVE_LLONG |
| /000001.sz/Volume | 107663 | H5T_NATIVE_UINT |
| /000002.sz/AskOrder | 76527 | H5T_NATIVE_UINT |

<br />通过观察其dataset信息，可以看出其字段为：AskOrder，BSFlag，BidOrder，BizIndex，Channel，FunctionCode，Index，OrderKind，Price，Time，Volume，对应数据表，我们加上证券代码Code和时期信息Date，所以最终的字段信息如下：

| name | type |
| ------ | ------ |
| Date | INT |
| Code | STRING |
| Volume | LONG |
| Time | LONG |
| Price | FLOAT |
| OrderKind | STRING |
| Index | LONG |
| FunctionCode | STRING |
| Channel | LONG |
| BizIndex | LONG |
| BidOrder | LONG |
| BSFlag | STRING |
| AskOrder | LONG |


## 2. HDF5插件
### 2. 1 插件导入
导入HDF5格式的数据需要用到DolphinDB HDF5插件。当dolphindb安装完成后，在安装目录下的/server/plugins/hdf5文件夹内，存放有插件的安装文件。使用如下代码导入DolphinDB HDF5插件，其中变量DlophinDBDir定义了dolphindb的安装目录，执行时，需要更换为自己的内容。
```txt
//需要根据自己的安装目录修改
DlophinDBDir = "/home/ychan/dolphindb"
loadPlugin(DlophinDBDir + "/server/plugins/hdf5/PluginHdf5.txt")
```
<br />插件导入也可以使用模块加载的方式，在全新环境中，配置配置preloadModules=plugins::hdf5，启动DolphinDB后，就可以直接使用HDF5插件。
### 2. 2 常用函数
关于插件函数介绍详见[插件教程](https://gitee.com/dolphindb/DolphinDBPlugin/tree/master)

## 3. 建库建表
### 3.1 DolphinDB的分区机制

DolphinDB利用分布式文件系统实现数据库的存储和基本事务机制。数据库以分区（chunk）为单位进行管理。分区的元数据（元数据指数据库的分区信息，每个分区的版本链，大小，存储位置等）存储在控制节点，副本数据存储在各数据节点，统一由分布式文件系统进行管理。一个数据库的数据可能存储在多个服务器上，系统内部通过事务机制和二阶段提交协议保证数据的强一致性和完整性，对于外部用户来说，这些机制是完全透明的。每个分区副本的数据采用列式增量压缩存储。压缩算法采用了LZ4方法，对金融数据平均能达到20%-25%的无损压缩比。

一个数据库最多可以支持三个维度的分区，支持百万甚至千万级的分区数。为尽可能保证每个分区的大小平衡，DolphinDB提供了值（VALUE）分区，范围（RANGE）分区，哈希（HASH）分区，列表（LIST）分区和复合（COMPO）分区等多种分区方式，用户可以灵活使用，合理规划分区。在查询时，加载数据的最小单位是一个分区的一个列。DolphinDB不提供行级的索引，而是将分区作为数据库的物理索引。一个分区字段相当于数据表的一个物理索引。如果查询时用到了该分区字段做数据过滤，SQL引擎就能快速定位需要的数据块，而无需对整表进行扫描。在量化金融领域，查询分析大多基于某一个时间段、某个产品标识进行，因此时间和产品标识是量化金融领域最常用的分区维度。有关分区的具体细节请参阅[分区数据库教程](./database.md)。

### 3.2 数据库设计

行情数据是量化金融中量级最大的数据类别。在中国证券市场，每日新增的数据在20-40G左右，累积的历史数据在20-40T左右。传统的关系型数据库处理这样的数据量级的性能非常低下。即使分库分表，效果也不理想。DolphinDB的分区机制可以轻松应对几百TB甚至PB级别的数据量。

为保证最佳性能，尽量将数据均匀分区，且将每个表的每个分区的数据量控制在压缩前100M左右。这是因为DolphinDB并不提供行级的索引，而是将分区作为数据库的物理索引，因此每个分区的数据量不宜过大。

行情数据通常可用时间和产品标识两个维度来进行分区：

(1) 时间维度大部分情况下可以选择按天进行值分区。如果时间跨度不是很长，而每天的数据量又非常大，也可以考虑按照小时进行分区，为此DolphinDB提供了DATEHOUR这种数据类型。设计分区机制时要考虑常用的应用场景。譬如说每次的请求都是对单一股票进行查询或聚合计算，而且跨越的时间比较长，可能几个月甚至一年，那么时间维度上按月分区不失为一种好的做法。

(2) 产品标识维度的分区可采用哈希、范围、值、列表等多种方法。如果每个产品在固定时间内的数据量比较均匀，可采用哈希或范围分区。例如中国的期货与股票市场以固定频率发布报价和交易的快照，因此每个市场内不同产品的数据量基本一致。美国金融市场的行情数据分布则完全不同，不同股票的tick级别数据量差异非常大。这种情境下，可选择范围分区，以一天或多天的数据为样本，将产品标识划分成多个范围，使得每一个范围内的产品的数据总量比较均衡。如果产品个数比较少，譬如期货的品种比较少，也可以考虑用值分区。

行情数据包括每日数据(end of day data)、Level 1、Level 2、Level 3等不同级别的数据。不同级别的数据，数据量差异比较大。所以建议采用不同分区机制的数据库来存储这些数据。

DolphinDB中的多个分区维度并不是层级关系，而是平级的组合关系。如果时间维度有n个分区，产品维度有m个分区，最多可能有n x m个分区。

K线数据或相关的signal数据都是基于高精度的行情数据降低时间精度产生的数据。通常，我们会生成不同频率的K线，譬如1分钟、5分钟、30分钟等等。这些不同频率的K线数据，因为数据量不是太大，建议存储在同一个分区表中，可以增加一个字段frequency来区分不同的时间窗口。K线表通常也按照日期和产品标识两个维度来分区，分区的粒度由数据量决定。以中国股票市场的分钟级K线为例，3000个股票每天产生约240个数据点，总共约72万个数据点。建议时间维度按月进行分区，产品的维度按范围或哈希分成15个分区。这样每个分区的数据量在100万行左右。这样的分区方法，既可在较长时间范围内（1个月或1年）快速查找某一个股票的数据，也可应对查找一天内全部股票的数据这样的任务。
### 3.3 建库建表脚本

遵循每个表每个分区中的常用数据压缩前为100MB左右的原则，可将数据库设计为复合分区，第一个维度按天（时间戳列）进行值分区，第二个维度按产品标识（证券代码列）分为20个HASH分区。由于数据库的数据表的创建都需要一定权限，本案例默认使用"admin"和"123456"作为用户名和密码登录，后续相关登录代码会省略。结合本范例的两种情况，给出了两建库建表方法，一种会删除原有库表，重新建立；另一种会先检测库表是否存在，如果存在，就不再建库建表，如果不存在，则先建库建表。
*  **按天分割建库建表**
   <br />此脚本建库建表时，会把原有数据删除，全部重新写入。
```txt
login(`admin,`123456)
if (existsDatabase("dfs://dataImportTrans"))
{
	dropDatabase("dfs://dataImportTrans")
}
db1 = database("", VALUE, 2020.01.01..2020.12.31)
db2 = database("", HASH,[SYMBOL,20])
db = database("dfs://dataImportTrans",COMPO, [db1,db2])
colNames=`TradeDate`Type`Seq`ExchID`SecID`ExTime`LocalTime`TradeTime`TradePrice`Volumn`Turnover`TradeBuyNo`TradeSellNo`TradeFlag
colTypes=[DATE,INT,INT,SYMBOL,SYMBOL,TIME,TIME,TIME,LONG,LONG,DOUBLE,INT,INT,SYMBOL]
t=table(1:0,colNames,colTypes)
transpt=db.createPartitionedTable(t,transpt,TradeDate`SecID)
```
*  **证券代码和列名分割建库建表**  
<br />此脚本要先检测数据库或数据表是否存在，如果不存在就建立，存在的话，就跳过建立的代码。为此，用例提供如下两个自定义函数。
```txt
def createDBIfNotExists() {
	if(existsDatabase("dfs://huachuang02")) return
	dbDate = database("", VALUE, 2021.07.01..2021.07.31)
	dbCode = database("", HASH, [SYMBOL, 20])
	db = database("dfs://huachuang02", COMPO, [dbDate, dbCode])
}

def createTableIfNotExists() {
	if(existsTable("dfs://huachuang02", "test")) return 
	name = ["Date", "Code", "Time", "Side", "Price", "OrderItems", "ABItems", "ABVolume0", "ABVolume1", "ABVolume2", "ABVolume3", "ABVolume4", "ABVolume5", "ABVolume6", "ABVolume7", "ABVolume8", "ABVolume9", "ABVolume10", "ABVolume11", "ABVolume12", "ABVolume13", "ABVolume14", "ABVolume15", "ABVolume16", "ABVolume17", "ABVolume18", "ABVolume19", "ABVolume20", "ABVolume21", "ABVolume22", "ABVolume23", "ABVolume24", "ABVolume25", "ABVolume26", "ABVolume27", "ABVolume28", "ABVolume29",  "ABVolume30", "ABVolume31", "ABVolume32", "ABVolume33", "ABVolume34", "ABVolume35", "ABVolume36", "ABVolume37", "ABVolume38", "ABVolume39",  "ABVolume40", "ABVolume41", "ABVolume42", "ABVolume43", "ABVolume44", "ABVolume45", "ABVolume46", "ABVolume47", "ABVolume48", "ABVolume49"]
	type = [DATE, SYMBOL, LONG, LONG, FLOAT, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG]
	model = table(1:0, name, type)
	database("dfs://huachuang02").createPartitionedTable(model, "test", `Date`Code)
}
```
## 4. 数据导入
### 4.1 按天的HDF5文件导入
当我们要读取HDF5数据时，我们首先要了解此文件的dataset是怎样的，我们使用lsTable函数获取所有table信息，先读取测试文件20200708.h5的table信息。
```txt
dataFilePath = "/hdd/hdd1/hdf5/tianfeng/20200708.h5"
datasetTable = hdf5::lsTable(dataFilePath)
```
datasetTable结果如下：

| tableName | tableDims | tableType |
| ------ | ------ | ------ |
| /StockTransaction/20200708 | 74802474 | H5T_COMPOUND |

通过观察tableName，可以发现这个HDF5文件是以天来分割存储数据的。于是我们以tableName的任意一个值做为参数，使用extractHDF5Schema函数去查看数据集的结构。

```txt
datasetName = "/StockTransaction/20200708"
schema = hdf5::extractHDF5Schema(dataFilePath,datasetName)
```
schema的结果如下：

| name | type
| ------ | ------ |
| TradeDate | INT |
| Type | INT |
| Seq | INT |
| ExID | STRING |
| SecID | STRING |
| ExTime | INT |
| LocalTime | INT |
| TradeTime | INT |
| TradePrice | LONG |
| Volume | LONG |
| Turnover | DOUBLE |
| TradeBuyNo | LONG |
| TradeSellNo | INT |
| TradeFlag | STRING |

name列可以做为分布式表的column。对于type列，其中的时间日期字段，TradeDate，ExTime，LocalTime，TradeTime被识别成了int，和数据库的时间日期不符合。在数据的导入函数loadHDF5Ex中，有一个参数，解释如下。

- tranform: 一元函数，并且该函数接受的参数必须是一个表。如果指定了transform参数，需要先创建分区表，再加载数据，程序会对数据文件中的数据执行transform参数指定的函数，再将得到的结果保存到数据库中。

根据此参数，我们定义一个类型转换的函数，在写入时，把int型转成需要的时间日期类型。

```txt
def  typeConversion(mutable t){
	return t.replaceColumn!(`TradeDate,datetimeParse(string(t.TradeDate),"yyyyMMdd")).replaceColumn!(`LocalTime,datetimeParse(string(t.LocalTime),"HmmssSSS")).replaceColumn!(`TradeTime,datetimeParse(string(t.TradeTime),"HmmssSSS")).replaceColumn!(`ExTime,datetimeParse(string(t.ExTime),"HmmssSSS"))
}
```
然后，就可以逐个导入数据了。
```txt
lst=hdf5::lsTable(dataFilePath)
for(datasetName in lst[`tableName])    
{
	hdf5::loadHDF5Ex(db,`transpt,`TradeDate`SecID,dataFilePath,datasetName,,,,typeConversion)
}
```
导入完成后，可以查看使用数据
```txt
transpt=loadTable("dfs://dataImportTrans","transpt")
select count(*) from transpt group by ExID,SecID
select  * from transpt where secID=`600009 
select sum(Volume) as v1, sum(Volume * (Type=110)) as bv,sum(Volume * (Type=1)) as sv from  transpt where secID=`600009
```

这个脚本结合按天分割的建库建表，每次执行都会删除库表并重建，本案例的完整脚本[点击下载]( script/HDF5_Import_demo/hdf5_by_day.dos),脚本执行前要确保已经导入过HDF5插件,数据[点击下载]()
### 4.2 按字段的HDF5文件导入
有些数据把一支股票的一列存成了一个dataset，并且忽略了日期列，日期用文件名给出。对于这种数据，对于每支证券对需要一个字段一个字段的取出。然后，拼接成一个可以导入数据库的表。同样地，也用lsTable函数先查看一下，由于结果比较大，只查看前12行。

```txt
filePath = "/hdd/hdd1/cwj/20210712.h5"
t_dataset = hdf5::lsTable(filePath)
t_dataset[:12]
```
结果如下：

| tableName | tableDims | tableType |
| ------ | ------ | ------ |
| /000001.sz/AskOrder | 107663 | H5T_NATIVE_UINT |
| /000001.sz/BSFlag | 107663 | H5T_STRING |
| /000001.sz/BidOrder | 107663 | H5T_NATIVE_UINT |
| /000001.sz/BizIndex | 107663 | H5T_NATIVE_ULLONG |
| /000001.sz/Channel | 107663 | H5T_NATIVE_UINT |
| /000001.sz/FunctionCode | 107663 | H5T_STRING |
| /000001.sz/Index | 107663 | H5T_NATIVE_UINT |
| /000001.sz/OrderKind | 107663 | H5T_STRING |
| /000001.sz/Price | 107663 | H5T_NATIVE_FLOAT |
| /000001.sz/Time | 107663 | H5T_NATIVE_LLONG |
| /000001.sz/Volume | 107663 | H5T_NATIVE_UINT |
| /000002.sz/AskOrder | 76527 | H5T_NATIVE_UINT |

通过tableName列可以看出，这个字段内包含了证券代码和表字段名称，并且没有日期的描述。对应数据库表，我们要增加日期列，这个列的内容从文件名中获取，并把tableName分解为证券代码和列名两部分。代码分步讲解如下：

*  这句代码是取出日期，这个日期会做为一列写入
```txt
dt = temporalParse(split(filePath, "/").last().split(".").first(), "yyyyMMdd")
```
*  列出文件中的dataset，返回的是一个表，数据有三列，列名是tableName，tableDims，tableType
```txt
t_dataset = hdf5::lsTable(filePath)
```
*  dataset的tableName字段中第一到第九位是股票代码，从第十一位到最后是字段名称，把它们拆出来，做为两列附加到t_dataset，生成新表tb1
```txt
tb1 = select substr(tableName, 1, 9) as `code, substr(tableName, 11) as `colName, * from t_dataset
```
*  把股票代码去重，得到所有且唯一的股票代码，存入向量code
```txt
codes = exec distinct code from tb1
```
如上分析，每个dataset是一列，我们首先要把这些列组合成一个表，再写入数据库的分布式表，所以要根据分布式表的结构创建一个存放数据的内存表，代码如下
*  获取分布式表结构
```txt
t_dfs = loadTable("dfs://huachuang02", "test")
```
* 根据分布式表的结构，创建内存表，下面按代码逐个读取数据，存入这个内存表
```txt
t_to_dfs = table(1000000:0, t_dfs.schema().colDefs.name, t_dfs.schema().colDefs.typeInt)
```
接下来遍历向量codes中的所有证券代码，根据证券代码逐个读取数据，下面的代码都在for (cd in codes){}循环体内
* t_tmp 是某个代码下面的所有字段
```txt
	t_tmp = select * from tb1 where code = cd
```
* 获取这个代码下的文件名，名字格式是代码加列名
```txt
	tables = exec tableName from t_tmp
* 获取所有字段名，这个字段名和上面的文件名是一一对应的
```txt
	cols = exec colName from t_tmp
* 根据字段数量生成一个临时数组，数组的每个元素是一个向量
```txt
	vec_tmp = array(ANY, tables.size())
* 如果tableDims 为0，即表为空表，则不加载数据
```txt
	if ((exec tableDims from t_tmp)[0] == "0") 
	{
		continue
	}
```
* 按照代码和字段逐个导入数据，并且按顺序放入vec_tmp
```txt
	for (i in 0 : tables.size()) 
	{
		vec_tmp[i] = hdf5::loadHDF5(filePath, tables[i]).rename!(cols[i])
	}
```
* 一列或多列为一表，多表合并为一表
```txt
	t = reduce(table, vec_tmp)
```
* 添加Date、Code列，并填充数据
```txt
	addColumn(t, `Date`Code, [DATE, SYMBOL])
	replaceColumn!(t, `Date, take(dt, t.size()))
	replaceColumn!(t, `Code, take(cd, t.size()))
```
* 调整列顺序并写入临时内存表
```txt
	reorderColumns!(t, t_dfs.schema().colDefs.name)
	t_to_dfs.append!(t)
```
到这里，for (cd in codes){}循环题就结束了，所有数据也写入了t_to_dfs这个内存表
* 最后把这个内存表写入数据库，就完成了数据导入
```txt
t_dfs.append!(t_to_dfs)
```
这个脚本结合按证券代码和列名分割的情形，数据库和数据表在每次执行时，都不删除，完整的脚本代码[点击下载]( script/HDF5_Import_demo/hdf5_by_column.dos)，脚本执行前要确保已经导入过HDF5插件，用例数据[点击下载]

### 4.3 按字段的HDF5文件导入，并且字段列数可能不同
在章节4.1的例子中，每个文件只存储了一列，但是，有些hdf5的文件其中的某些dataset，会在章节4.1的基础上保存多列数据，本例中，证券代码+ABVolume的dataset就保存50列数据。所以在上例的基础上，当读取ABVolume文件时，我们要把它分别写入50列，并对这50列逐个命名。首先定义一个50列名字的向量
```txt
ABVolume = ["ABVolume0", "ABVolume1", "ABVolume2", "ABVolume3", "ABVolume4", "ABVolume5", "ABVolume6", "ABVolume7", "ABVolume8", "ABVolume9", "ABVolume10", "ABVolume11", "ABVolume12", "ABVolume13", "ABVolume14", "ABVolume15", "ABVolume16", "ABVolume17", "ABVolume18", "ABVolume19", "ABVolume20", "ABVolume21", "ABVolume22", "ABVolume23", "ABVolume24", "ABVolume25", "ABVolume26", "ABVolume27", "ABVolume28", "ABVolume29",  "ABVolume30", "ABVolume31", "ABVolume32", "ABVolume33", "ABVolume34", "ABVolume35", "ABVolume36", "ABVolume37", "ABVolume38", "ABVolume39",  "ABVolume40", "ABVolume41", "ABVolume42", "ABVolume43", "ABVolume44", "ABVolume45", "ABVolume46", "ABVolume47", "ABVolume48", "ABVolume49"]
```
在读取文件时做判断，如果读取的是ABVolume，则用ABVolume这个向量对数据重命名，如果不是ABVolume，则保持和上例一样，用对应的列名来命名。
```txt
if (cols[i] == "ABVolume") 
{
	vec_tmp[i] = hdf5::loadHDF5(filePath, tables[i]).rename!(ABVolume)
}
else 
{
	vec_tmp[i] = hdf5::loadHDF5(filePath, tables[i]).rename!(cols[i])
}
```
除了这部分内容外，其余部分和章节4.1一致，完整的脚本代码[点击下载](script/HDF5_Import_demo/hdf5_by_column_Volume.dos)，数据[点击下载]()
## 5.  NPZ数据导入
npz数据是numpy中一种常用的数据格式，在dlophindb中使用loadNpz函数可以非常方便地导入这种数据。如下是Python np.array 和 DolphinDB 对象对照表：
| numpy array | ddb obj |
| ---| ------ |
| 一维 | 向量 |
|二维 | 矩阵 |
| 三维 | 元组，元组的每一个元素是一个矩阵 |
支持转换的数据类型有：bool, char, short, int, long, float, double, string(只支持一维)。loadNpz函数执行结束后返回一个字典，字典的key是对象名，对应到数据库中就是列名；字典的value是转化好的数据对象。在把返回的数据对象写入到数据表时，一维向量对应一个普通列，二维矩阵对应一列arrayVector，三维的元组对应多列arrayVector。下面代码导入了一个npz文件。

```
tmp = loadNpz("/home/ychan/data/amber/sample-binance-btcusdt-perp-orderbook.npz")
tmp.keys()
asks[0].shape()
bids[0].shape()
```
查看返回字典的key可以得到，这个文件所包含的3个字段：bids，asks，local_timestamp。其中local_timestamp，是一个时间维度的向量。bids和asks都是矩阵组成的元组，通过执行asks[0].shape()和bids[0].shape()可以得到，每个矩阵是3列100行。结合原始文件的定义，矩阵的第一列是price字段，第二列是size字段，第三列是size_in_coin字段，在这些字段前面分别增加前缀bids和asks，得到了bids_price，bids_size，bids_size_in_coin，	asks_price，	asks_size，asks_size_in_coin六列字段，这些字段的字是一个长度为100的arrayVector，和local_timestamp组成列数为7的表，定义如下：
```
npz_table = table(100:0,`datetime`bids_price`bids_size`bids_size_in_coin`asks_price`asks_size`asks_size_in_coin,[NANOTIMESTAMP,DOUBLE[],DOUBLE[],DOUBLE[],DOUBLE[],DOUBLE[],DOUBLE[]])
```
接下来定义6个和asks（asks，bids，local_timestamp长度都是一样的）等长的数组
```
asks = tmp.asks
bids = tmp.bids
len = asks.size()
asks_price = array(ANY, len)
asks_size = array(ANY, len)
asks_size_in_coin = array(ANY, len)
bids_price = array(ANY, len)
bids_size = array(ANY, len)
bids_size_in_coin = array(ANY, len)
```
逐个解析元组，把其中的一列转为array，保存在上面定义数组的对应位置。
```
for(i in 0:len){
	m = asks[i]
	n = bids[i]
	bids_price[i] = n[0]
	bids_size[i] = n[1]
	print(bids_size[i])
	bids_size_in_coin[i] = n[2]
	asks_price[i] = m[0]
	asks_size[i] = m[1]
	asks_size_in_coin[i] = m[2]	
}
```
解析时间列，和上面的数组一起写入定义好的内存表
```
local_timestamp = tmp.local_timestamp
insert into npz_table values(local_timestamp,bids_price,bids_size,bids_size_in_coin,asks_price,asks_size,asks_size_in_coin)
```
然后，建库建表，把内存表加入到数据库
```
if(existsDatabase("dfs://db_amber_npz"))
{
	dropDatabase("dfs://db_amber_npz")
}

db_amber_npz = database(directory="dfs://db_amber_npz",partitionType=VALUE,partitionScheme=2020.02.21..2020.02.28,engine=`TSDB)
db_amber_npz.createPartitionedTable(table=npz_table,tableName=`npz,partitionColumns=`datetime,sortColumns=`datetime)

table_npz = loadTable("dfs://db_amber_npz",`npz)
table_npz.append!(npz_table)
```
完整的脚本代码[点击下载](script/HDF5_Import_demo/load_npz.dos)，数据[点击下载]()