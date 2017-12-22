# Topic Modeling on Apache Spark
This package contains a set of distributed text modeling algorithms implemented on Spark, including:

- **Online LDA**: an early version of the implementation was merged into MLlib (PR #4419), and several extensions (e.g., predict) are added here

- **Gibbs sampling LDA**: the implementation is adapted from Spark PRs(#1405 and #4807) and JIRA SPARK-5556 (https://github.com/witgo/spark/tree/lda_Gibbs, https://github.com/EntilZha/spark/tree/LDA-Refactor, https://github.com/witgo/zen/tree/lda_opt/ml, etc.), with several extensions (e.g., support for MLlib interface, predict and in-place state update) added

- **Online HDP (hierarchical Dirichlet process)**: implemented based on the paper "Online Variational Inference for the Hierarchical Dirichlet Process" (Chong Wang, John Paisley and David M. Blei)

- **Notes from Stephen Boesch December 2017**


This Repo lacked working code for the HDP.  I added an ```OnlineHDPExample``` program.  In addition the dependencies were udpated to Spark 2.2 and Scala 2.11 and latest Breeze (linear algebra library). 

To run the example:

```mvn exec:java -Dexec.mainClass="org.apache.spark.mllib.topicModeling.OnlineHDPExample" -Dexec.args="--master local  --stopwordFile src/main/resources/stopwords.txt --maxDocs 100 --maxIterations 2 /git/topmetrics/data/mininews"```

Note: *maven* is unable to stop the job properly - and so a spurious error message is generated at the end: something like:


```
Results
LDAMetrics(OnlineHDP,274,-2147.483648,2147.483647,List()),LDAMetrics(OnlineHDP,274,-2147.483648,2147.483647,List())
17/12/21 23:52:28 INFO AbstractConnector: Stopped Spark@5b3c8e38{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
17/12/21 23:52:28 WARN FileSystem: exception in the cleaner thread but it will continue to run
java.lang.InterruptedException
	at java.lang.Object.wait(Native Method)
	at java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:143)
	at java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:164)
	at org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner.run(FileSystem.java:2989)
	at java.lang.Thread.run(Thread.java:748)
[WARNING] thread Thread[org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner,5,org.apache.spark.mllib.topicModeling.OnlineHDPExample] was interrupted but is still alive after waiting at least 12891msecs
[WARNING] thread Thread[org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner,5,org.apache.spark.mllib.topicModeling.OnlineHDPExample] will linger despite being asked to die via interruption
[WARNING] NOTE: 1 thread(s) did not finish despite being asked to  via interruption. This is not a problem with exec:java, it is a problem with the running code. Although not serious, it should be remedied.
[WARNING] Couldn't destroy threadgroup org.codehaus.mojo.exec.ExecJavaMojo$IsolatedThreadGroup[name=org.apache.spark.mllib.topicModeling.OnlineHDPExample,maxpri=10]
java.lang.IllegalThreadStateException
	at java.lang.ThreadGroup.destroy(ThreadGroup.java:778)
	at org.codehaus.mojo.exec.ExecJavaMojo.execute(ExecJavaMojo.java:321)
	at org.apache.maven.plugin.DefaultBuildPluginManager.executeMojo(DefaultBuildPluginManager.java:134)
	at org.apache.maven.lifecycle.internal.MojoExecutor.execute(MojoExecutor.java:208)
	at org.apache.maven.lifecycle.internal.MojoExecutor.execute(MojoExecutor.java:154)
	at org.apache.maven.lifecycle.internal.MojoExecutor.execute(MojoExecutor.java:146)
	at org.apache.maven.lifecycle.internal.LifecycleModuleBuilder.buildProject(LifecycleModuleBuilder.java:117)
	at org.apache.maven.lifecycle.internal.LifecycleModuleBuilder.buildProject(LifecycleModuleBuilder.java:81)
	at org.apache.maven.lifecycle.internal.builder.singlethreaded.SingleThreadedBuilder.build(SingleThreadedBuilder.java:51)
	at org.apache.maven.lifecycle.internal.LifecycleStarter.execute(LifecycleStarter.java:128)
	at org.apache.maven.DefaultMaven.doExecute(DefaultMaven.java:309)
	at org.apache.maven.DefaultMaven.doExecute(DefaultMaven.java:194)
	at org.apache.maven.DefaultMaven.execute(DefaultMaven.java:107)
	at org.apache.maven.cli.MavenCli.execute(MavenCli.java:993)
	at org.apache.maven.cli.MavenCli.doMain(MavenCli.java:345)
	at org.apache.maven.cli.MavenCli.main(MavenCli.java:191)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.codehaus.plexus.classworlds.launcher.Launcher.launchEnhanced(Launcher.java:289)
	at org.codehaus.plexus.classworlds.launcher.Launcher.launch(Launcher.java:229)
	at org.codehaus.plexus.classworlds.launcher.Launcher.mainWithExitCode(Launcher.java:415)
	at org.codehaus.plexus.classworlds.launcher.Launcher.main(Launcher.java:356)
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

You can safely ignore that ```ThreadGroup.destroy``` error.
 
