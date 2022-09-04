##### 1 使用loadTextEx将数据导入分区数据表中

将数据导入分区表中，最高效的方法就是使用函数[loadTextEx](http://dolphindb.com/help/loadTextEx.html)。因为loadTextEx不需要将全部数据读取到内存，同时还可以实现并行导入。也就是说，当数据量大于内存时候，最适合使用loadTextEx。

```
//假设t.csv是输入文件
db = database("dfs://rangedb", RANGE, 0 50 100)

db.createPartitionedTable(`pt,`ID)

//将数据导入表"pt",分区列为"ID"，并将导入的表返回给变量t1
t1=loadTextEx(db,`pt,`ID,"/DolphinDB/Data/t.csv");
>t1;

ID date       x
-- ---------- --------
27 2017.08.07 0.774064
35 2017.08.07 2.590557
2  2017.08.10 6.637173
23 2017.08.10 0.994591
39 2017.08.11 1.51236
25 2017.08.10 4.358145
42 2017.08.07 7.450394
18 2017.08.07 1.682372
28 2017.08.08 2.815332
9  2017.08.09 6.542935
...

```


##### 2 将数据先倒入到内存，然后再通过[append!](http://dolphindb.com/help/append1.html) 将数据写入分区数据表

可以使用的将数据导入到内存的方法包括，[loadText](http://dolphindb.com/help/loadText.html), [ploadText](http://dolphindb.com/help/ploadText.html)（将数据并行读取到内存）

```
t1=loadText(db,`pt,`ID,"/DolphinDB/Data/t.csv");
db = database("dfs://rangedb", RANGE, 0 50 100)
db.createPartitionedTable(t1,`pt,`ID).append!(t1)      //以表t1作为模板创建分区表，表名为"pt",分区列名为"ID", 并将t1中的数据，写入到"pt"中
```


##### 3 用例分析

下面我们通过一个股票报价信息的大数据来做一个实例演示。假设我们有2007.8~2007.9两个月的股票报价数据(500GB)，每天是一个单独的csv文件（大概15GB），如果需要导入DolphinDB数据库可以采用如下方式。该数据包含如下字段

```
symbol  股票交易代码
date    报价日期
time    报价时间
bid     买方愿意出的最高价格
ofr     卖方愿意接受的最高价格
bidsiz  买方愿意买入最大股数
ofrsiz  卖方愿意卖出最大股数

```


##### 3.1 根据字段产生均匀分区

我们首先按日期进行第一级分区。然后再用股票交易代码进行第二层分区（根据数据分布）。

```
def prepPartitions(){
    WORK_DIR = "/DolphinDBDemo/INPUT_DATA"

     // 将数据并行导入
	t=ploadText(WORK_DIR+"/TAQ20070801.csv")

	// 选择2007.08.1这天的数据来计算股票交易代码的分布
	t=select count(*) as ct from t where date=2007.08.01 group by symbol

	// 通过分布数据，以及cutPoints函数，按照股票代码按字母顺序产生128个均匀区间。每个区间内部的股票的报价记录数是相当的。
	buckets = cutPoints(exec symbol from t, 128, exec ct from t)

	// 将最后一个区间结束设置成不会出现的最大的数值。
	buckets[size(buckets)-1]=`ZZZZZ

	// 将这些区间存放到一个表中
	t1=table(buckets as bucket)

	// 将表写到磁盘上以便后面使用
	t1.saveText(WORK_DIR+"/buckets20070801.txt")
}

prepPartitions()
```

##### 3.2 创建数据库

```

def createDB(){
    WORK_DIR = "/DolphinDBDemo/INPUT_DATA"
    if (existsDatabase("dfs://TAQ"))
        dropDatabase("dfs://TAQ")

     // 第一层分区
    db1 = database("", VALUE, 2007.08.01..2007.09.30)

    // 将前面由数据产生的股票代码分区导入
    partitions = exec bucket from loadText(WORK_DIR+'/buckets20070801.txt')

    // 用股票代码创建第二层分区
    db2 = database("", RANGE, partitions)

    // 创建数据库
    db = database("dfs://TAQ", HIER, [db1, db2])

    // 创建数据库分区表 quotes,table()函数用于初始化数据库字段和数据类型, 分区字段为date和symbol.
    db.createPartitionedTable(table(100:0, `symbol`date`time`bid`ofr`bidsiz`ofrsiz, [SYMBOL, DATE, SECOND, DOUBLE, DOUBLE, INT, INT]), `quotes, `date`symbol)
}

createDB()
```


##### 3.3 分区表写入

数据库写入，可以通过如下代码轻松实现。

```
def loadToDB(filedir){

    // 打开数据库
	db = database("dfs://TAQ")

	// 将文件载入分区表quotes中： db是指向数据库TAQ句柄， quotes是分区表名，`date`symbol是分区字段列表，filedir是文本文件路径。
	loadTextEx(db, "quotes", `date`symbol,filedir )
}

WORK_DIR = "/DolphinDBDemo/INPUT_DATA"

 // 将TAQ20070801.csv写入数据库
loadToDB(WORK_DIR+"/TAQ20070801.csv")
```


##### 3.4 分区表并行写入

这里分两种情况载入数据。

###### 假设数据文件分布在不同的节点的相同路径（/VOL2/INPUT），并且每个节点的需要加载的文件不重复。

```
def loadJob(){
	filenames = exec filename from files('/VOL2/INPUT')
	db = database("dfs://TAQ")
	filedir = '/VOL2/INPUT'
	for(fname in filenames){
		jobId = fname.strReplace(".csv", "")
		jobName = jobId

		// submitJob: 提交一个后台运行的任务。jobId: 任务的ID前缀名, jobName: 任务名/任务简介， loadTextEx是需要执行的函数，这里用花括号"{}"来包含需要使用到的参数。
		submitJob(jobId,jobName, loadTextEx{db, "quotes", `date`symbol,filedir+'/'+fname})
	}
}

 // pnodeRun将一个函数发送到每个节点上执行
pnodeRun(loadJob)
```



###### 假设每个节点都有所有需要载入的数据文件。

```
WORK_DIR = "/DolphinDBDemo/INPUT_DATA"

//提取需要处理的文件名列表
csvFiles = exec filename from files(WORK_DIR) where filename like "TAQ2007%.csv" order by filename

//需要执行并发写入的节点。
nodes = ["P1-NODE1","P2-NODE1","P3-NODE1","P5-NODE1"]

 //初始化并发任务ID数组
jobs = array(string,0)
nodeIdx = 0
db = database("dfs://TAQ")
for (csvFile in csvFiles) {
	jobId = "load_" + csvFile.strReplace(".csv", "")
	jobName = jobId
	node = nodes[nodeIdx]

	//  rpc在远端节点上执行一个任务。这个任务会返回一个唯一的jobId，可以用来进行状态查询。
	jobId = rpc(node,submitJob,jobId,jobName,loadToDB, WORK_DIR + "/" +csvFile)

    // 添加任务ID到数组以备后面查询执行状态
	jobs.append!(jobId)
	nodeIdx += 1
	if(nodeIdx>=size(nodes)){
		nodeIdx = 0
	}
}

```

查询数据库并发写入进程

```
 // 在每个节点上执行getRecentJobs是取得最近执行的任务信息。
pnodeRun(getRecentJobs)

// 或者
select * from pnodeRun(getRecentJobs) where jobId in jobs  //如果你将每个任务的ID存放到了数组jobs中。

```
