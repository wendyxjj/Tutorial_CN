# DolphinDB函数式编程

 - [1、自定义函数](#1、自定义函数)
 - [2、纯函数](#2、纯函数)
 - [3、部分应用](#3、部分应用)
 - [4、高阶函数调用](#4、高阶函数调用)
 - [5、Lambda函数](#5、Lambda函数)
 - [6、闭包](#6、闭包)
 - [7、远程函数调用](#7、远程函数调用)
 - [8、模块和插件](#8、模块和插件)
 - [9、函数视图](#9、函数视图)
 - [10、jit](#10、jit)

函数式编程是一种编程范式，我们常见的编程范式有命令式编程，函数式编程，逻辑式编程，常见的面向对象编程是也是一种命令式编程。

在DolphinDB上，应用了许多的函数式编程思想。
* DolphinDB中很多的系统内置的功能函数，大部分功能实现了本地多线程并行或者多机分布式计算，减少了计算时间。
* 用户可以自定义函数，可以通过传递参数的方法进行动态命令执行，减少了重复代码的编写。
* 函数可以作为高阶函数的参数，在DolphinDB上，有下面的两种高阶函数。
    * 模板是一种内置的高阶函数，可以扩展或增强函数或者运算符的功能。模板以一个函数与数据对象作为输入内容。通常，输入数据首先以一种预设的方式被分解成多个数据块（可能重叠），然后将函数应用于每个数据块，最后将所有的结果组合为一个对象返回。模板的输入数据可以是向量、矩阵或者表，也可以是标量或字典。通过使用模板，某些复杂的分析任务可以用几行代码就可以高效地完成。
    * 远程函数调用也是一类高阶函数，可以把调用了其他本地函数（内置函数或用户定义函数）的本地函数发送到远程节点即时运行，无需编译或者部署。系统自动把函数定义和所有相关函数的定义以及所需的本地数据序列化，并发送到远程节点。
## 1、自定义函数
DolphinDB脚本支持自定义一些函数，然后调用。通过函数调用的形式就很方便通过不同的参数来动态执行函数，减少了重复代码的编写。在DolphinDB上有许多有本地多线程优化和分布式优化的函数，可以通过在自定义函数中使用这些函数，可以极大提高运行效率。        
* DolphinDB函数参数通过引用传递。
* 函数体内只能引用函数参数和函数内的局部变量，不能使用函数体外定义的变量。
* 当输入参数没有限定符时，在函数体中则不能修改该参数。
* 当参数用mutable修饰时，在函数体中可以修改该参数。
* 用户自定义聚合函数返回的数据格式为标量。有时候我们想确保一个函数返回的是一个标量，使用聚合函数就可以达到这个目的。用户自定义聚合函数和命名函数语法基本一致，不同之处在于用户自定义聚合函数的定义以"defg"开头而不是"def"。
```
def f(a){return a+1};
f(2);

def f1(mutable a){a+=1;return a};
f1(2);
3

defg f2(x, y){
a = sum(abs(x+y))
b=sum(abs(x))+sum(abs(y))
return a\b
};
x=-5..5;
y=0..10;
>f(x,y);

```
## 2、纯函数
纯函数是函数式编程中的一个重要概念，就是在普通函数的基础上，规定了传入参数不可更改，不使用外部的变量，即只有可读和计算的功能，在相同的输入情况下，结果都是一致的。函数即不依赖外部的状态也不修改外部的状态，函数调用的结果不依赖调用的时间和位置，这样写的代码容易进行推理，不容易出错。这使得单元测试和调试都更容易。

实际上，在DolphinDB脚本中就是参数不带mutable限定符的函数。相应地，如果真的需要使用外部变量，可以通过把外部变量作为实际参数传递给函数来运行，利于定位外部变量的使用位置，益于后期维护。

## 3、部分应用
部分应用是指固定一个函数的部分参数，产生一个参数较少的函数。当使用新产生的函数时，实际效果就如同实际调用填入已经固定参数的函数，即此时的函数是有状态的。
```
>def f(a,b):a*b;
>g=f{10};
>g(10);
100
```
>函数传参是引用传递，而在函数参数部分应用时是值传递，新产生的函数会保存一个数值列表的副本。
```
>def f(a,b):a*b;
>a=10;
>g=f{a};
>g(10);
100
a=100;
g(10);
100
```
>如果只是想固定前几个参数，那么不需要指明剩余的参数；如果在已被固定的参数前有未固定参数，必须指明这些未固定参数的位置。

## 4、高阶函数调用
高阶函数是函数式编程中的重要概念，函数是一等公民，普通函数可以作为高阶函数的参数，函数就会在DolphinDB中的高阶函数进行调用。
dolphindb中有许多不同的高阶函数，需要函数的名称和对应的参数，就会根据原本设置的功能，按一定逻辑反复执行传入的函数。
传入的参数是单一常量，向量，表以及矩阵，高阶函数会按照特定的逻辑规则来执行传入的函数对象。
### 4.1 all
把函数应用到参数的每一个元素。只要函数调用返回false，all模板就会停止执行，并返回false。如果所有的函数调用都返回true，all模板将返回true。
### 4.2 any
与all类似,
把函数应用到参数的每一个元素。只要函数调用返回true，any模板就会停止执行，并返回true。如果所有的函数调用都返回false，any模板将返回false。
### 4.3 call
用指定的参数调用一个函数。常常用在each/peach或loop/ploop中，用来调用一批函数。常用于函数部分辅助。
### 4.4 cross (:C)/pcross

语法:`cross(func, X, [Y])` 或 `X <operator>:C Y`

不同于前面的高阶函数all,any和call，cross的参数允许不对称。

将X和Y中元素的两两组合作为参数来调用函数。如果X或Y是矩阵，以列为单位遍历。以下是cross 模板的伪代码：

``` shell
for(i:0~(size(X)-1)){
   for(j:0~(size(Y)-1)){
       result[i,j]=<function>(X[i], Y[j]);
   }
}
```

return result;

### 4.5 each (:E) / peach
语法：
`each(func, args...)`
或
`F :E X`
或
`X <operator>:E Y`

把一个函数应用到X和Y中的每个元素，X和Y长度相同。

peach为each的并行版本。

### 4.6 eachLeft (:L)

语法: `eachLeft(func, X, Y)` 或
`X <operator>:L Y`

把func(X(i),Y)应用到X的每个元素中。


### 4.7 eachPre (:P)

语法:`eachPre(func, X, [pre])` 或
`[pre] <operator>:P X`

将给定函数/运算符应用于X中所有相邻的数据对。

eachPre 模板等同于： F(X[0], pre), F(X[1], X[0]), ..., F(X[n], X[n-1]).

### 4.8 eachRight (:R)

语法:`eachRight(func, X, Y)` 或
`X <operator>:R Y`

把func(X, Y(i))应用到Y中的每个元素。
### 4.9 eachPost (:O)

语法:`eachPost(func, X, [post])` 或
`<operator>:O Y` 或 `Y <operator>:O X`

将给定函数/运算符应用到所有相邻的数据对上。

eachPost 模板等同于：F(X[0], X[1]), F(X[1], X[2]), ..., F(X[n], post).

### 4.10 loop / ploop

loop 模板与each模板很相似，区别在于函数返回值的格式和类型。
相反，loop没有这样的限制。

ploop为loop的并行版本。

### 4.11 accumulate (:A)

accumulate模板先应用函数/运算符在init和X[0]上，再迭代地应用到前一个结果和X中的下一个元素上。
### 4.12 reduce (:T)
reduce 模板先将函数/运算符运用在init和X[0]上，再迭代地应用到前一个结果和X中的下一个元素上。与accumulate返回中间结果不同，reduce 只返回最后一个结果。
### 4.13 groupby (:G)
在每个分组中计算func(funcArgs)。每组的计算结果可以是标量，向量或字典，该模板的输出结果是一个表，它的行数与分组数相等。
#### 语法
groupby(func, funcArgs, groupingCol)

或

funcArg func:G groupingCol
### 4.14 contextby (:X)
根据groupingCol分组，并在组内进行func(funcArgs)运算。返回结果是一个与输入参数长度相同的向量。如果func是聚合函数，每组内的所有结果相同。若指定了sortingCol，在计算前，依此列进行组内排序。
### 4.15 segmentby
根据segment参数确定分组方案，把funcArgs分组，并把函数func应用到每个分组中。返回的结果与segment参数的长度相同。
### 4.16 moving
应用函数/运算符到给定对象的一个滚动窗口上。
### 4.17 rolling
应用函数/运算符到给定对象的一个滑动窗口上。
### 4.18 pivot
在指定的二维维度上重组数据，结果为一个矩阵。
### 4.19 pcall
将输入参数分成几个部分，并行计算，最后将结果合并。如果输入参数的长度小于100,000，pcall函数不会并行计算。
## 5、Lambda函数
Lambda 表达式是只有一个语句的函数。
## 6、闭包
跟函数参数部分应用类似，清晰明了的分层关系。但是闭包只能通过自定义函数提前设置好闭包的函数结构，而且使用该函数时需要填写两层参数。
```
def g(a){return def(b): a pow b};  
f=g(2);
f(3);
```
>闭包参数的传递也是值传递，在函数对象中保存了数值副本。
## 7、远程函数调用
远程函数调用是分布式系统常用的功能之一。 DolphinDB的远程函数调用最强大的功能是，我们可以把调用了其他本地函数（内置函数或用户定义函数）的本地函数发送到远程节点即时运行，无需编译或者部署。系统自动把函数定义和所有相关函数的定义以及所需的本地数据序列化，并发送到远程节点。其他一些系统不能远程调用与用户定义函数相关的函数。远程函数调用也是函数式编程的应用之一，把所要执行的逻辑操作封装成一个函数，然后把该函数和所需参数作为一个普通的函数参数，很简单地在远端运行函数。

我们可以使用remoteRun和rpc函数来远程调用函数。它们的区别是：
* rpc利用集群中数据节点之间现有的异步连接；remoteRun使用xdb函数创建的显式连接。
* rpc的调用节点和远程节点必须在同一集群；remoteRun没有这样的限制。
### 7.1使用remoteRun执行远程函数
DolphinDB使用`xdb`创建一个到远程节点的连接。远程节点可以是任何运行DolphinDB的节点，不必属于当前集群的一部分。创建连接之后可以在远程节点上执行远程节点上注册的函数或本地自定义的函数。
```
h = xdb("localhost", 8081);
```
在远程节点上执行一段脚本：

```
remoteRun(h, "sum(1 3 5 7)");
16
```
上述远程调用也可以简写成：

```
h("sum(1 3 5 7)");
16
```

在远程节点上执行一个在远程节点注册的函数：

```
h("sum", 1 3 5 7);
16
```

在远程系节点上执行本地的自定义函数：

```
def mysum(x) : reduce(+, x)
h(mysum, 1 3 5 7);
16
```

> 如果本地的自定义函数有依赖，所依赖的自定义函数会自动序列化到远程节点

### 7.2 使用rpc执行远程函数

DolphinDB使用远程过程调用功能的另一个途径是`rpc`函数。`rpc`函数接受远程节点的名称，需要执行的函数定义以及需要的参数。`rpc`只能在同一个集群内的控制节点及数据节点之间使用，但是不需要创建一个新的连接，而是复用已经存在的网络连接。这样做的好处是可以节约网络资源和免去创建新连接带来的延迟。当节点的用户很多时，这一点非常有意义。`rpc`函数只能在远程节点执行一个函数。如果要运行脚本，请把脚本封装在一个自定义函数内。
下面的例子必须在一个DolphinDB集群内使用。nodeB是远程节点的别名。

```
rpc("nodeB", sum, 1 3 5 7);
```

## 8、模块和插件
### 8.1 模块
在DolphinDB中，模块是指只包含函数定义的脚本文件。当需要调用一个特定的函数时，可以通过调用含有该函数的模块来实现。通过module语句声明一个模块，该语句位于模块文件的第一行。
* 模块按层组织。命名结构必须与目录结构相一致。在下面模块声明的例子中，module语句放在了模块文件的首行。
* 使用.dos作为模块文件的后缀，是"dolphin script"的缩写。
### 8.2 插件
模块和插件就是函数接口的概念，让用户不必知道具体的函数实现，只需要了解函数的功能就可以调用这个接口来实现功能。
dolphindb中可以编写自定义插件，具体方式就是写出与一个extern "C" ConstantSP sendEmail(Heap *heap, vector<ConstantSP> &args)的函数，
，然后与给出的libDolphinDB进行编译成一个动态库，并且需要一个让dolphindb server识别函数的一个txt文件。
编译出来的动态库可以在dolphindb中动态加载，然后就可以使用脚本来执行与之对应的函数，后续无需进行编译，执行效率接近于直接使用c++编写的程序。

## 9、函数视图
函数视图就是就是高级用户可以给一个用户一个权限，某个特定的函数可以访问某区域的数据，这样就不需要有完整的数据访问就可以实现数据的计算等功能。

下面的例子中，自定义函数getSpread计算dfs://TAQ/quotes 表中指定股票的平均买卖报价差。用户user1不具有读取dfs://TAQ/quotes 表的权限。现在我们把函数getSpread定义为函数视图，并赋予用户user1执行该视图的权限。虽然user1不具备读取dfs://TAQ/quotes 表的权限，但是仍然可以通过执行getSpread函数，利用dfs://TAQ/quotes 表的数据进行计算，来获得指定股票的买卖报价差。
>由于dfs://TAQ/quotes 是分布式数据库，以下代码需要由系统管理员在控制节点上执行。用户user1可在任意数据节点运行getSpread函数。
```
def getSpread(s, d){

   return select avg((ofr-bid)/(ofr+bid)*2) as spread from loadTable("dfs://TAQ","quotes") where symbol=s, date=d

}
addFunctionView(getSpread)  
grant("user1", VIEW_EXEC, "getSpread")
```
如果DolphinDB集群重启，之前定义的函数视图仍然可以使用。但是DolphinDB不允许直接修改函数视图中的语句，如果要修改函数视图，需要先使用dropFunctionView函数删除函数视图。
```
dropFunctionView("getSpread")
```
## 10、jit
即时编译(英文: Just-in-time compilation, 缩写: JIT)，又译及时编译或实时编译，是动态编译的一种形式，可提高程序运行效率。在运行时将代码翻译为机器码，可以达到与静态编译语言相近的执行效率。

DolphinDB的编程语言是解释执行，运行程序时首先对程序进行语法分析生成语法树，然后递归执行。在不能使用向量化的情况下，解释成本会比较高。这是由于DolphinDB底层由C++实现，脚本中的一次函数调用会转化为多次C++内的虚拟函数调用。for循环，while循环和if-else等语句中，由于要反复调用函数，十分耗时，在某些场景下不能满足实时性的需求。
> 目前jit仅支持自定义函数，需要整个自定义函数中的引用函数还有数据类型都需要支持jit。
```
@jit
def myFunc(/* arguments */) {
  /* implementation */
}

```