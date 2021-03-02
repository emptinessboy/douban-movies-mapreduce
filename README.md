### MapReduce数据清洗

#### 数据上传HDFS：

先将python爬取到的数据上传到我在Hadoop平台的个人文件夹下的文档目录中。

[![2018010587-__10__0003.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/01/2018010587-__10__0003.jpg)](https://up.media.everdo.cn/image/oKmO)
[![2018010587-__11__0001.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/01/2018010587-__11__0001.jpg)](https://up.media.everdo.cn/image/oa7n)

完成将数据从Windows宿主机上传到Linux虚拟机 后，需要 将Linux上的文件上传到Hadoop平台（HDFS）上面。

这一步的操作可以使用hdfs -dfs 或者hadoop fs 命令来完成。

[![2018010587-__11__0002.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/01/2018010587-__11__0002.jpg)](https://up.media.everdo.cn/image/ogqS)

命令执行完毕后，可以到HDFS提供的在线 浏览工具中查看下当前HDFS中的数据。

[![2018010587-__11__0003.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/01/2018010587-__11__0003.jpg)](https://up.media.everdo.cn/image/oZL9)

可以看到，爬取得到的数据已经成功 上传到分布式文件系统HDFS上了。

### **新建MapReduce项目：**

由于我较为习惯使用IDEA集成开发环境，因此这里我在Ubuntu虚拟机 上安装来IDEA开发工具，并使用了
IDEA来创建MapReduce项目。因为IDEA没有像Eclipse那样有Hadoop的插件来简化开发，本实验的项目
结构也较为简单，因此不依赖Maven之类的构建工具，直接新建一个普通的JAVA程序项目。

[![2018010587-__12__0001.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/01/2018010587-__12__0001.jpg)](https://up.media.everdo.cn/image/oUzG)

项目创建完成后，还需要导入Hadoop以及MapReduce依赖的相关包。

[![2018010587-__12__0002.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/01/2018010587-__12__0002.jpg)](https://up.media.everdo.cn/image/oOb3)

#### 导入依赖：

由于后续我还需要在项目中引入com.alibaba.FASTJSON等自定义或者第三方的包，因此我在项目目录下新建
了一个lib文件夹，用来存放第三方或者自定义的jar包。

接下来下载反序列化需要用到的阿里巴巴开源的JSON库（com.alibaba.fastjson）

[![2018010587-__13__0001.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/01/2018010587-__13__0001.jpg)](https://up.media.everdo.cn/image/oGFj)

导入FASTJSON依赖：

[![2018010587-__13__0002.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/01/2018010587-__13__0002.jpg)](https://up.media.everdo.cn/image/oLO6)

#### 清洗爬取的JSON文件

由于Mapper阶段我们需要让程序从HDFS读取所有 存放在input文件夹中的json文件。 而电影 的排名信息是以json的文件名来命名的。所以map阶段先要 通过InputSplit对象来获取文件名，然后 使用正则表达式提取电影排名信息。

获取排名信息后，我使用阿里巴巴的fastJSON将文本的JSON文件来进行反序列化操作。然后从中很方便的提取我们需要的信息存放到数组中。

提取完成信息后，我们对数组进行一次循环判空来过滤无效信息，最后使用将数据转为text类型并作为key输出：context.write(text, NullWritable.get());

**Map阶段代码：**

[![2018010587-__14__0001.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__14__0001.jpg)](https://up.media.everdo.cn/image/24HJ)

[![2018010587-__14__0002.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__14__0002.jpg)](https://up.media.everdo.cn/image/olXQ)

[![2018010587-__15__0001.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__15__0001.jpg)](https://up.media.everdo.cn/image/2Hby)

[![2018010587-__15__0002.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__15__0002.jpg)](https://up.media.everdo.cn/image/2J0d)

**Reduce阶段代码：**

由于数据清洗的过程主要处理的是key，Map阶段没有产生需要处理的value。因此reduce阶段可以直接输出。

[![2018010587-__15__0004.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__15__0004.jpg)](https://up.media.everdo.cn/image/2hF0)

#### 执行阶段代码：

这部分代码主要是指定Mapper类，Reduce类，指定文件的输入输出信息以及期交MapReduce作业。

[![2018010587-__15__0003.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__15__0003.jpg)](https://up.media.everdo.cn/image/2PLl)

**打包并运行MapReduce：**

![2018010587-__16__0001.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__16__0001.jpg)

先配置打包编译的参数，下一步就可以点击构建按钮输出jar包了。

[![2018010587-__16__0002.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__16__0002.jpg)](https://up.media.everdo.cn/image/2R7c)

打包完成后，使用hadoop jar命令运行我的MR程序， 效果如下：

[![2018010587-__16__0003.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__16__0003.jpg)](https://up.media.everdo.cn/image/27OL)

运行完成：

[![2018010587-__17__0001.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__17__0001.jpg)](https://up.media.everdo.cn/image/2jx2)

查看运行结果：

[![2018010587-__17__0002.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__17__0002.jpg)](https://up.media.everdo.cn/image/2oXZ)

运行完成后可以通过浏览HDFS的输出文件夹查看效果。

将文件下载到本地进行查看， 可以看到MapReduce清洗后，剔除来JSON中无用的信息，并按照一行一部电影的方式，使用分隔符 “|” 进行分割。

[![2018010587-__18__0001.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__18__0001.jpg)](https://up.media.everdo.cn/image/2FHk)

至此，数据清洗结束。

### 遇到的问题以及解决方案：

#### AUTO_TYPE异常

遇到抛出异常 **com.alibaba.fastjson.JSONException: autoType is not support** ，解决方案参考官网文档的开
启AUTO_TYPE方法。

[![2018010587-__18__0002.jpg](https://media.everdo.cn/tank/pic-bed/2021/03/02/2018010587-__18__0002.jpg)](https://up.media.everdo.cn/image/23dB)
