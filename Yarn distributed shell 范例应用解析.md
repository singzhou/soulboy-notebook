# Yarn distributed shell 官方范例应用解析

作为官网给出的yarn application示例，这个示例很好讲述了如何撰写一个yarn分布式应用的客户端和Application Master，对于初学者很有学习意义，该应用在Hadoop官方代码仓中，位置如下：

![](http://image.huawei.com/tiny-lts/v1/images/9c032697a232be339050638936e059f5_653x595.png)

该应用本身的作用是提交用户写的一条shell命令或者shell script到yarn集群，然后由application master申请资源执行。虽然功能不难，但是这个小应用的代码量也来到了几千行，写应用看得出来不是一个很简单调几个API的事。网上例子也比较少，找来找去还是这个例子最好。这篇文章从源码的角度，然后结合一些中间值的dump（序列化其实都是基于proto实现的），来梳理从提交到执行的全部过程。

## 执行

首先你需要有这个distributed shell项目的jar包，如果是你下载了源码可以直接编译获得（不过需要一个有公司代理地址的mvn setting文件从而你可以从中心仓下载你编译需要的jar包，没有的找同事要一下），或者如果你直接下载的二进制文件，你可以直接在`hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.1.3.jar`找到这个jar包

![image-20240912164223050](http://image.huawei.com/tiny-lts/v1/images/3e6fa2420cf2019961cb800119a5de45_414x902.png)

获得jar包后，执行如下命令：

```shell
yarn jar hadoop-yarn-applications-distributedshell.jar org.apache.hadoop.yarn.applications.distributedshell.Client -jar hadoop-yarn-applications-distributedshell.jar -shell_command "echo Hello YARN!" -num_containers 2
```

这个命令还有很多参数，参数都在Client的构造器里定义了，感兴趣的可以自己去换一些玩一玩。这里的例子就是打印一行Hello Yarn。



## Client

从main函数看起，并没有什么东西，对于YarnClient的创建都被封装在了Client，主要的逻辑都放在了`run`函数中。这里的`init`函数主要是对用户指令的解析，很繁琐，这里就不解析了，`run`方法中主要就是用了init函数解析出来的结果。

```java
/**
   * @param args Command line arguments 
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      Client client = new Client();
      LOG.info("Initializing Client");
      try {
        boolean doRun = client.init(args);
        if (!doRun) {
          System.exit(0);
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        client.printUsage();
        System.exit(-1);
      }
      result = client.run();
    } 
    // 省略...
  }
```

`run`函数主要干了这么几件事（下面的代码去除了一些不关心的代码）

**（1）集群信息收集**

这段代码主要获取了yarn集群的一些状态信息，调度队列的信息，acl信息，总的来说就是在传应用之前先查看一下集群的情况

```java
yarnClient.start();
YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
```

这些变量可以用debug简单看看内容：

![image-20240912173102678](http://image.huawei.com/tiny-lts/v1/images/2f8fc94169dad0d2d360a43067ae7f16_1889x375.png)

**（2）资源配置和验证**

这部分主要是设置了应用所需的资源需求，并在yarn集群中验证其可行性，说白了就是看看资源够不够。

```java
YarnClientApplication app = yarnClient.createApplication();
// 这里集群返回一个response，包括集群的资源情况，然后我们拿出来看看是不是符合用户所定义的资源需求。
GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
long maxMem = appResponse.getMaximumResourceCapability().getMemorySize();
int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
// ...对比资源剩余和用户想申请的
```

response可以dump出来打开看一看，更加方便理解：

```
{
  "application_id": {
    "id": 39,
    "cluster_timestamp": 1723105433913
  },
  "maximumCapability": {
    "memory": 8192,
    "virtual_cores": 4,
    "resource_value_map": [
      {
        "key": "memory-mb",
        "value": 8192,
        "units": "Mi",
        "type": "COUNTABLE"
      },
      {
        "key": "vcores",
        "value": 4,
        "units": "",
        "type": "COUNTABLE"
      }
    ]
  }
}

```

**（3）应用上下文设置**

这段主要设置应用的提交上下文，包括应用 ID、资源类型、容器保留策略、应用名称、标签等。

```java
ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
ApplicationId appId = appContext.getApplicationId();

// Set up resource type requirements
// For now, both memory and vcores are supported, so we set memory and
// vcores requirements
List<ResourceTypeInfo> resourceTypes = yarnClient.getResourceTypeInfo();
setAMResourceCapability(appContext, profiles, resourceTypes);
setContainerResources(profiles, resourceTypes);

appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
appContext.setApplicationName(appName);

if (attemptFailuresValidityInterval >= 0) {
    appContext
        .setAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
}

Set<String> tags = new HashSet<String>();
// tags设置省略...
appContext.setApplicationTags(tags);
```

相当于我们尝试获得应用的上下文对象，然后接着再往里设置一些东西，最后这个上下文dump出来大概长这个样子：

```
{
  "application_id": {
    "id": 39,
    "cluster_timestamp": 1723105433913
  },
  "application_name": "DistributedShell",
  "keep_containers_across_application_attempts": false,
  "am_container_resource_request": {
    "priority": {
      "priority": 0
    },
    "resource_name": "*",
    "capability": {
      "memory": 100,
      "virtual_cores": 1,
      "resource_value_map": [
        {
          "key": "memory-mb",
          "value": 100,
          "units": "Mi",
          "type": "COUNTABLE"
        },
        {
          "key": "vcores",
          "value": 1,
          "units": "",
          "type": "COUNTABLE"
        }
      ]
    },
    "num_containers": 1,
    "relax_locality": true,
    "execution_type_request": {
      "execution_type": "GUARANTEED",
      "enforce_execution_type": false
    }
  }
}

```



**（4）本地资源配置**

配置应用程序所需的本地资源，如 `AppMaster.jar` 和相关脚本。此部分还包括从本地复制文件到 HDFS，以便应用执行时使用。

```java
// set local resources for the application master
// local files or archives as needed
// In this scenario, the jar file for the application master is part of the local resources
Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

// Copy the application master jar to the filesystem 
// Create a local resource to point to the destination jar path 
FileSystem fs = FileSystem.get(conf);
addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(),
                    localResources, null);

// Set the log4j properties if needed 
if (!log4jPropFile.isEmpty()) {
    addToLocalResources(fs, log4jPropFile, log4jPath, appId.toString(),
                        localResources, null);
}			

// The shell script has to be made available on the final container(s)
// where it will be executed. 
// To do this, we need to first copy into the filesystem that is visible 
// to the yarn framework. 
// We do not need to set this as a local resource for the application 
// master as the application master does not need it.
// 这里比较有意思，它认为如果希望执行一个脚本文件，
// 并不需要以local resource的形式暴露给AM，传到HDFS就可以了。
// 这里我理解AM只要知道执行的文件名和一些参数信息，如下面的shell Args变量
// 说白了AM只要负责告诉container执行什么命令就可以了，脚本文件本身能被yarn知道就行了。
// 这也间接说明对于AM物料的安排，yarn希望使用者尽量简介高效，不要什么都一股脑都放进去。
String hdfsShellScriptLocation = ""; 
long hdfsShellScriptLen = 0;
long hdfsShellScriptTimestamp = 0;
if (!shellScriptPath.isEmpty()) {
    Path shellSrc = new Path(shellScriptPath);
    String shellPathSuffix =
        appName + "/" + appId.toString() + "/" + SCRIPT_PATH;
    Path shellDst =
        new Path(fs.getHomeDirectory(), shellPathSuffix);
    fs.copyFromLocalFile(false, true, shellSrc, shellDst);
    hdfsShellScriptLocation = shellDst.toUri().toString(); 
    FileStatus shellFileStatus = fs.getFileStatus(shellDst);
    hdfsShellScriptLen = shellFileStatus.getLen();
    hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();
}

if (!shellCommand.isEmpty()) {
    addToLocalResources(fs, null, shellCommandPath, appId.toString(),
                        localResources, shellCommand);
}

if (shellArgs.length > 0) {
    addToLocalResources(fs, null, shellArgsPath, appId.toString(),
                        localResources, StringUtils.join(shellArgs, " "));
}
```

在我的这个使用例子中，AM物料的准备也比较简单，只包含了jar包和指令：

![image-20240912191304049](http://image.huawei.com/tiny-lts/v1/images/5748ae9b01c5109db966f60777f687ab_2057x76.png)

其实说是传Local Resource，我们根据proto的具体内容可以发现其实本质是传递一些元数据或者一些较为简单的信息，大物料如jar本身都是直接放到HDFS去了，我们把resource的proto打开来看看：

```
resource {
  scheme: "hdfs"
  host: "yhug"
  port: 9000
  file: "/user/root/DistributedShell/application_1723105433913_0039/AppMaster.jar" // 这个jar就是项目的jar，换了个名字
}
size: 111095
timestamp: 1726137989844
type: FILE
visibility: APPLICATION

resource {
  scheme: "hdfs"
  host: "yhug"
  port: 9000
  file: "/user/root/DistributedShell/application_1723105433913_0039/shellCommands"
}
size: 18
timestamp: 1726137989907
type: FILE
visibility: APPLICATION


```

其实都是一些元信息，包括我们自己传入的一行指令其实也是被写成了一个脚本文件里传到HDFS了。



**（5）环境变量和类的路径设置**

下面是一些执行AM所必要的环境变量

```java
// Set the env variables to be setup in the env where the application master will be run
LOG.info("Set the environment for the application master");
Map<String, String> env = new HashMap<String, String>();

// put location of shell script into env
// using the env info, the application master will create the correct local resource for the 
// eventual containers that will be launched to execute the shell scripts
env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION, hdfsShellScriptLocation);
env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP, Long.toString(hdfsShellScriptTimestamp));
env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN, Long.toString(hdfsShellScriptLen));
if (domainId != null && domainId.length() > 0) {
	env.put(DSConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN, domainId);
}

// Add AppMaster.jar location to classpath
// At some point we should not be required to add 
// the hadoop specific classpaths to the env. 
// It should be provided out of the box. 
// For now setting all required classpaths including
// the classpath to "." for the application jar
StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
for (String c : conf.getStrings(
    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
    YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
    classPathEnv.append(c.trim());
}
classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");

// add the runtime classpath needed for tests to work
if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
    classPathEnv.append(':');
    classPathEnv.append(System.getProperty("java.class.path"));
}

env.put("CLASSPATH", classPathEnv.toString());
```

环境变量的map如下图所示：

![image-20240912194049786](C:\Users\z00830407\AppData\Roaming\Typora\typora-user-images\image-20240912194049786.png)

把classpath打开来看看，主要是配置hadoop一些关键的jar包地址：

```
CLASSPATH -> {{CLASSPATH}}<CPS>./*<CPS>{{HADOOP_CONF_DIR}}<CPS>{{HADOOP_COMMON_HOME}}/share/hadoop/common/*<CPS>{{HADOOP_COMMON_HOME}}/share/hadoop/common/lib/*<CPS>{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/*<CPS>{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/lib/*<CPS>{{HADOOP_YARN_HOME}}/share/hadoop/yarn/*<CPS>{{HADOOP_YARN_HOME}}/share/hadoop/yarn/lib/*<CPS>./log4j.properties
```



**（6）AM的命令构建**

构建 `ApplicationMaster` 的启动命令，包括设置 JVM 相关参数（如内存限制）、传递给主节点的参数以及标准输出、错误输出的重定向。

```java
// Set java executable command 
LOG.info("Setting up app master command");
// Need extra quote here because JAVA_HOME might contain space on Windows,
// e.g. C:/Program Files/Java...
vargs.add("\"" + Environment.JAVA_HOME.$$() + "/bin/java\"");
// Set Xmx based on am memory size
vargs.add("-Xmx" + amMemory + "m");
// Set class name 
vargs.add(appMasterMainClass);
// Set params for Application Master

// ...省略一堆命令的堆加

vargs.addAll(containerRetryOptions);

// 这里主要是成功或失败输出到不同的日志里去
vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

// Get final commmand
StringBuilder command = new StringBuilder();
for (CharSequence str : vargs) {
    command.append(str).append(" ");
}
List<String> commands = new ArrayList<String>();
commands.add(command.toString());
```

我这个例子输出的command长这个样子：

```
"{{JAVA_HOME}}/bin/java" -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5016 -Xmx100m org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster --container_type GUARANTEED --container_memory 10 --container_vcores 1 --num_containers 2 --priority 0 1><LOG_DIR>/AppMaster.stdout 2><LOG_DIR>/AppMaster.stderr 
```



**（7）安全相关和容器上下文配置**

这部分代码处理安全凭证的配置和传递，并最终设置 `ApplicationMaster` 容器的启动上下文。该上下文包括本地资源、环境变量、启动命令等。这里我是没有设置的。

```java
// Setup security tokens
Credentials rmCredentials = null;
if (UserGroupInformation.isSecurityEnabled()) {
    // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
    rmCredentials = new Credentials();
    String tokenRenewer = YarnClientUtils.getRmPrincipal(conf);
    if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException(
            "Can't get Master Kerberos principal for the RM to use as renewer");
    }

    // For now, only getting tokens for the default file-system.
    final Token<?> tokens[] =
        fs.addDelegationTokens(tokenRenewer, rmCredentials);
    if (tokens != null) {
        for (Token<?> token : tokens) {
            LOG.info("Got dt for " + fs.getUri() + "; " + token);
        }
    }
}

// Add the docker client config credentials if supplied.
Credentials dockerCredentials = null;
if (dockerClientConfig != null) {
    dockerCredentials =
        DockerClientConfigHandler.readCredentialsFromConfigFile(
        new Path(dockerClientConfig), conf, appId.toString());
}

if (rmCredentials != null || dockerCredentials != null) {
    // ...
}
```



**（8）提交**

最后，向 YARN 提交应用程序并启动监控过程，跟踪应用程序的运行状态。

```java
// Set up the container launch context for the application master
// 这里把刚刚的那些本地资源啊，环境变量，执行命令全部放到这个container launch context里
ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);

// ...省略一些代码
appContext.setAMContainerSpec(amContainer);

// Set the priority for the application master
Priority pri = Priority.newInstance(amPriority);
appContext.setPriority(pri);

// Set the queue to which this application is to be submitted in the RM
appContext.setQueue(amQueue);

specifyLogAggregationContext(appContext);

// Submit the application to the applications manager
// SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
// Ignore the response as either a valid response object is returned on success 
// or an exception thrown to denote some form of a failure
yarnClient.submitApplication(appContext);

// Monitor the application
return monitorApplication(appId);

```



### 小结

虽然这个Yarn API使用是相对繁琐的，但是官方示例还是给我们提供了一定的编写一个Yarn应用客户端的思路，按照代码的逻辑，可以简单总结为这么几个步骤：

**集群信息收集**：收集节点、队列、用户权限等信息。

**资源配置与验证**：根据集群能力调整资源请求。

**应用程序上下文设置**：配置应用提交的元信息。

**资源配置**：准备和上传启动应用AM所需的本地资源、或者整个集群任务所需要的物料。

**环境变量与类路径**：设置运行时所需的类路径和环境变量。

**命令构建与安全设置**：构建应用主节点命令并设置安全凭证。

**应用提交与监控**：提交应用并监控其运行状态。



## ApplicationMaster

与客户端类似，先看主函数，里面东西不多，方法都被封装到了`run`方法中

```java
public static void main(String[] args) {
    boolean result = false;
    try {
      ApplicationMaster appMaster = new ApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
      result = appMaster.finish();
    } catch (Throwable t) {
      LOG.error("Error running ApplicationMaster", t);
      LogManager.shutdown();
      ExitUtil.terminate(1, t);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.error("Application Master failed. exiting");
      System.exit(2);
    }
}
```

AM的`run`函数主要干了这么几件事：

**（1）初始化和安全凭证处理**

设置和初始化当前 `ApplicationMaster` 运行时的安全凭证，并确保容器无法访问 `ApplicationMaster` 和 `ResourceManager` 之间的认证令牌。

```java
Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
// 移除 AM->RM token
Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
while (iter.hasNext()) {
  Token<?> token = iter.next();
  if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
    iter.remove();
  }
}
appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
appSubmitterUgi.addCredentials(credentials);

```



**（2）RM和NM客户端初始化**

启动与 `ResourceManager` 和 `NodeManager` 的通信客户端，并初始化资源管理器客户端。这里提到了handler的概念，对应了异步的概念，以召回函数的方式处理从RM或者NM传回的信息。这两个handler会在后面提到。

```JAVA
AMRMClientAsync.AbstractCallbackHandler allocListener =
        new RMCallbackHandler();
amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
amRMClient.init(conf);
amRMClient.start();

containerListener = createNMCallbackHandler();
nmClientAsync = new NMClientAsyncImpl(containerListener);
nmClientAsync.init(conf);
nmClientAsync.start();

```



**（3）向RM注册AM**

向 `ResourceManager` 注册 `ApplicationMaster`，并接收集群资源的相关信息，如内存、cpu等。感觉和客户端向Yarn提交应用非常相似。

```java
// Register self with ResourceManager
// This will start heartbeating to the RM
appMasterHostname = NetUtils.getHostname();
Map<Set<String>, PlacementConstraint> placementConstraintMap = null;
if (this.placementSpecs != null) {
    placementConstraintMap = new HashMap<>();
    for (PlacementSpec spec : this.placementSpecs.values()) {
        if (spec.constraint != null) {
            placementConstraintMap.put(
                Collections.singleton(spec.sourceTag), spec.constraint);
        }
    }
}
RegisterApplicationMasterResponse response = amRMClient
    .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                               appMasterTrackingUrl, placementConstraintMap);
resourceProfiles = response.getResourceProfiles();
ResourceUtils.reinitializeResources(response.getResourceTypes());
// Dump out information about cluster capability as seen by the
// resource manager
// AM也需要向RM询问资源，同时调整资源申请。
long maxMem = response.getMaximumResourceCapability().getMemorySize();
LOG.info("Max mem capability of resources in this cluster " + maxMem);

int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
LOG.info("Max vcores capability of resources in this cluster " + maxVCores);

// A resource ask cannot exceed the max.
if (containerMemory > maxMem) {
    LOG.info("Container memory specified above max threshold of cluster."
             + " Using max value." + ", specified=" + containerMemory + ", max="
             + maxMem);
    containerMemory = maxMem;
}

if (containerVirtualCores > maxVCores) {
    LOG.info("Container virtual cores specified above max threshold of cluster."
             + " Using max value." + ", specified=" + containerVirtualCores + ", max="
             + maxVCores);
    containerVirtualCores = maxVCores;
}
```

我们可以看看这里RM给AM返回了什么，和之前客户端向yarn请求非常类似，对比一下，是不是可以理解为客户端向Yarn申请需要拉起AM的资源，而AM是申请继续执行应用所需的资源：

```
maximumCapability {
  memory: 8192
  virtual_cores: 4
  resource_value_map {
    key: "memory-mb"
    value: 8192
    units: "Mi"
    type: COUNTABLE
  }
  resource_value_map {
    key: "vcores"
    value: 4
    units: ""
    type: COUNTABLE
  }
}
queue: "default"
scheduler_resource_types: MEMORY
resource_profiles {
}
resource_types {
  name: "memory-mb"
  units: "Mi"
  type: COUNTABLE
}
resource_types {
  name: "vcores"
  units: ""
  type: COUNTABLE
}
```



**（4）容器分配和资源请求**

检查上次尝试中尚未完成的容器，并请求新容器执行任务。

**步骤**：

1. 检查上次 `ApplicationMaster` 尝试中的容器状态，将它们加入已分配容器列表。上一次尝试指例如崩溃、超时或系统故障而失败，Yarn 允许应用程序进行重新尝试。
2. 计算还需请求的容器数目。
3. 向 `ResourceManager` 请求新的容器，直到所有任务容器被分配。（这个步骤是异步的，申请提交上去函数就会返回，然后调用用户提供的召回函数处理）。

```java
List<Container> previousAMRunningContainers = response.getContainersFromPreviousAttempts();
for(Container container: previousAMRunningContainers) {
  launchedContainers.add(container.getId());
}
int numTotalContainersToRequest = numTotalContainers - previousAMRunningContainers.size();
for (int i = 0; i < numTotalContainersToRequest; ++i) {
  ContainerRequest containerAsk = setupContainerAskForRM();
  amRMClient.addContainerRequest(containerAsk);
}
```



至此`run`的内容结束了，因为所有RM的请求都是异步调用，所以接下来需要看看AM和RM之间的召回函数、AM和NM的之间的召回函数做了什么。不管是AMRM之间的异步请求处理，还是NM的异步请求处理，Yarn都为这俩种客户端提供了callback handler模板，用户可以实现这些模板callback函数从而处理一系列RM或者NM返回给AM的结果。

**RMCallbackHandler**

下面的代码依旧省略了一些片段，只关注一些重要的逻辑

```java
class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
    
    // 当容器的内容处理完后，需要做什么
    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Got response from RM for container ask, completedCnt="
          + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        // 根据Container传回的status进行一些解析，例如检查一下任务是否成功，退出码是什么。
        String message = appAttemptID + " got container status for containerID="
            + containerStatus.getContainerId() + ", state="
            + containerStatus.getState() + ", exitStatus="
            + containerStatus.getExitStatus() + ", diagnostics="
            + containerStatus.getDiagnostics();
        
        // ...省略一些任务状态的的逻辑检查，成功与否。

        // increment counters for completed/failed containers
        // 根据退出码看看shell是不是执行成功了，然后管理一下成功或者失败的container数量，管理的逻辑下面注释写的很清楚了。
        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          // container failed
          if (ContainerExitStatus.ABORTED != exitStatus) {
            // shell script failed
            // counts as completed
            numCompletedContainers.incrementAndGet();
            numFailedContainers.incrementAndGet();
          } else {
            // container was killed by framework, possibly preempted
            // we should re-try as the container was lost for some reason
            numAllocatedContainers.decrementAndGet();
            numRequestedContainers.decrementAndGet();
            // we do not need to release the container as it would be done
            // by the RM

            // Ignore these containers if placementspec is enabled
            // for the time being.
            if (placementSpecs != null) {
              numIgnore.incrementAndGet();
            }
          }
        } else {
          // nothing to do
          // container completed successfully
          numCompletedContainers.incrementAndGet();
          LOG.info("Container completed successfully." + ", containerId="
              + containerStatus.getContainerId());
        }
        // ...省略一些timeline服务
      }

      // ask for more containers if any failed
      // 重新申请一些容器，用于重试容器被杀掉的、容器执行失败的一些情况。
      int askCount = numTotalContainers - numRequestedContainers.get();
      numRequestedContainers.addAndGet(askCount);

      // Dont bother re-asking if we are using placementSpecs
      if (placementSpecs == null) {
        if (askCount > 0) {
          for (int i = 0; i < askCount; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM();
            amRMClient.addContainerRequest(containerAsk);
          }
        }
      }

      if (numCompletedContainers.get() + numIgnore.get() >=
          numTotalContainers) {
        done = true;
      }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      for (Container allocatedContainer : allocatedContainers) {
        if (numAllocatedContainers.get() == numTotalContainers) {
          // 如果申请到了但是发现数量其实已经够了，那就释放还回去
          // ...省略一些代码
        } else {
          // 如果正常申请到了需要的容器，那就开始执行shell
          numAllocatedContainers.addAndGet(1);
          String yarnShellId = Integer.toString(yarnShellIdCounter);
          yarnShellIdCounter++;
            
          // ...省略一些日志代码
		 
          // 注意这里每个容器的任务触发是非阻塞的，会开一条新线程去专门为申请到的容器准备执行指令和物料。
          // 这里LaunchThread做了什么会稍后展开分析。
          Thread launchThread =
              createLaunchContainerThread(allocatedContainer, yarnShellId);

          // launch and start the container on a separate thread to keep
          // the main thread unblocked
          // as all containers may not be allocated at one go.
          launchThreads.add(launchThread);
          launchedContainers.add(allocatedContainer.getId());
          launchThread.start();

          // Remove the corresponding request
          // ...省略一些代码，如果申请成功了就把成员变量中的对应的请求删除掉了。
          }
        }
      }
    }

    @Override
    public void onContainersUpdated(
        List<UpdatedContainer> containers) {
      for (UpdatedContainer container : containers) {
        // ...省略一些代码
        // ...如果资源有更新就调用NM的客户端去更新容器的资源。
        nmClientAsync.updateContainerResourceAsync(container.getContainer());
      }
    }

    @Override
    public void onRequestsRejected(List<RejectedSchedulingRequest> rejReqs) {
      // ...省略了一些代码，这里主要是给被申请拒绝的请求尝试重试。
    }

    @Override public void onShutdownRequest() {
      // 主要是一些日志打印
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
      // 很明显就是一个进度的计算
      // set progress to deliver to RM on next heartbeat
      float progress = (float) numCompletedContainers.get()
          / numTotalContainers;
      return progress;
    }

    @Override
    public void onError(Throwable e) {
      LOG.error("Error in RMCallbackHandler: ", e);
      done = true;
    }
  }
```

然后看一下非阻塞启动容器任务的逻辑`LaunchThread`

```java
  /**
   * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
   * that will execute the shell command.
   */
  private class LaunchContainerRunnable implements Runnable {

    // ...成员变量

    // 主要看一下run方法做了什么
    @Override
    /**
     * Connects to CM, sets up container launch context 
     * for shell command and eventually dispatches the container 
     * start request to the CM. 
     */
    public void run() {

      // Set the local resources
      // 准备物料
      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

      // The container for the eventual shell commands needs its own local
      // resources too.
      // In this scenario, if a shell script is specified, we need to have it
      // copied and made available to the container.
      if (!scriptPath.isEmpty()) {
        // ...省略一些物料准备的代码
        LocalResource shellRsrc = LocalResource.newInstance(yarnUrl,
          LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
          shellScriptPathLen, shellScriptPathTimestamp);
        localResources.put(Shell.WINDOWS ? EXEC_BAT_SCRIPT_STRING_PATH :
            EXEC_SHELL_STRING_PATH, shellRsrc);
        shellCommand = Shell.WINDOWS ? windows_command : linux_bash_command;
      }

      // Set the necessary command to execute on the allocated container
      // 准备执行指令
      Vector<CharSequence> vargs = new Vector<CharSequence>(5);

      // ...省略一些指令准备

      // Get final commmand
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }

      List<String> commands = new ArrayList<String>();
      commands.add(command.toString());
        
      // 这步很关键，准备ContainerLaunchContext，和之前客户端申请AM的启动上下文一样，AM启动其他容器的执行也需要准备上下文。
      // Set up ContainerLaunchContext, setting local resource, environment,
      // command and token for constructor.

      // Note for tokens: Set up tokens for the container too. Today, for normal
      // shell commands, the container in distribute-shell doesn't need any
      // tokens. We are populating them mainly for NodeManagers to be able to
      // download anyfiles in the distributed file-system. The tokens are
      // otherwise also useful in cases, for e.g., when one is running a
      // "hadoop dfs" command inside the distributed shell.
      Map<String, String> myShellEnv = new HashMap<String, String>(shellEnv);
      myShellEnv.put(YARN_SHELL_ID, shellId);
      ContainerRetryContext containerRetryContext =
          ContainerRetryContext.newInstance(
              containerRetryPolicy, containerRetryErrorCodes,
              containerMaxRetries, containrRetryInterval,
              containerFailuresValidityInterval);
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
        localResources, myShellEnv, commands, null, allTokens.duplicate(),
          null, containerRetryContext);
      containerListener.addContainer(container.getId(), container);
      // 这里调用了NM客户端，正式启动了容器的任务
      nmClientAsync.startContainerAsync(container, ctx);
    }
  }
```

所以可以看到整个启动container任务的逻辑链是这样的：

（1）AM向RM异步申请容器。

（2）RM申请成功，调用AM的RM Handler召回函数。

（3）在召回函数中非阻塞的启动线程去准备申请到的容器的任务触发。

（4）一个容器一条线程，准备物料和容器启动指令放入容器启动的上下文，然后调用NM的客户端把容器启动。



**NMCallbackHandler**

刚刚我们对RM的召回函数进行了一些逻辑分析，可以发现其实里面其实是有用到NM的客户端的，因此NM显而易见也会有对应的召回函数，相对逻辑简单一点：

```java
@VisibleForTesting
  class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {

    // ...成员变量和构造器
    
    public void addContainer(ContainerId containerId, Container container) {
      // ...
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      // ,,,
      containers.remove(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
	  
      // ...省略一些日志
        
      // If promote_opportunistic_after_start is set, automatically promote
      // opportunistic containers to guaranteed.
      // ...对容器状态解析后做一些操作
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      // ...省略一些日志
      
      // 成功启动容器后去请求容器的状态。
      Container container = containers.get(containerId);
      if (container != null) {
        applicationMaster.nmClientAsync.getContainerStatusAsync(
            containerId, container.getNodeId());
      }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      // 启动失败处理
      LOG.error("Failed to start Container {}", containerId, t);
      containers.remove(containerId);
      applicationMaster.numCompletedContainers.incrementAndGet();
      applicationMaster.numFailedContainers.incrementAndGet();
      // ...
    }

    @Override
    public void onGetContainerStatusError(
        ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop Container " + containerId);
      containers.remove(containerId);
    }

    @Deprecated
    @Override
    public void onIncreaseContainerResourceError(
        ContainerId containerId, Throwable t) {}

    @Deprecated
    @Override
    public void onContainerResourceIncreased(
        ContainerId containerId, Resource resource) {}

    @Override
    public void onUpdateContainerResourceError(
        ContainerId containerId, Throwable t) {
    }

    @Override
    public void onContainerResourceUpdated(ContainerId containerId,
        Resource resource) {
    }
  }
```

至此AM对应用执行的资源申请和容器启动逻辑就结束了，简单总结一下的话AM和RM、NM的交互都是异步的，信息的交互反馈、处理和管理都是基于召回函数实现的。



## 总结

一趟分析下来最直观的感觉就是写一个Yarn应用真的是非常繁琐的事情，光是API的使用或许就已经足够让人头大，还不算你需要基于你应用的需求去定制很多的逻辑。市面上有一些简化Yarn应用开发的软件，比如Skein，通过配置文件的方式来描述一个Yarn应用。在Skein的文档中指出一个普通的Yarn应用基本需要6000行左右的代码量去实现，可见开发应用确实不易，这篇文章的初衷也是基于Yarn官方的例子也尝试去总结一些应用开发的逻辑大模板。

最后对于客户端，开发需要实现：

**集群信息收集**

**资源配置与验证**

**应用程序上下文设置**

**资源配置**

**环境变量与类路径**

**命令构建与安全设置**

**应用提交与监控**



对于AM，开发需要实现：

**AM向RM请求资源**

**AM向NM准备执行上下文**

**AM分别于RM、NM的召回函数（逻辑或许可以非常复杂）**

