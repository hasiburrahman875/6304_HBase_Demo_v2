# HBase_Demo

### Start HBase
Open a terminal and start the service
```
$HBASE_HOME/bin/start-hbase.sh
```
In another window if we write 'jps', we will see HMaster running.  
Your $HBASE_HOME is /opt/hbase.


### Importing Project:
* Open “eclipse”, right click on “Package Explorer” window, click import.
* Select “Git”-> “Projects from Git” and click “next”.
* Select “clone url” and click “next”.
* Paste “https://github.com/hasiburrahman875/CS6304_HBase_Demo” in the “url” textbox, and click “next”. 
* Choose “Import existing project” and click “finish”.

### Referencing HBase libraries:
* Right click on project and select “build path”-> “configure build path” ->”libraries”->”add external jars”.
* Go to "other location"(right side, last option)-> computer->opt->hbase->lib and select all jars.
* REMEMBER: there is a folder named "ruby", do not select it while selecting all the jars
* Add the jars and close

### How to run
Follow Commands.txt to run on terminal.
Java codes will run on eclipse directly after referencing HBase libraries.

### Stop HBase
Stop the service when required using below command
```
$HBASE_HOME/bin/stop-hbase.sh
```


### General Information (access your VM):
* Operating System:         Mac -> Microsoft Remote Desktop, Windows -> Default Remote Desktop, Ubuntu -> Remmina
* Machine:                  cs6304-<mst_username>-01.class.mst.edu
* User:                     <mst_username>
* Default Password:         <mst_password>




### Common error fix:
Error 1: ERROR [main] zookeeper.RecoverableZooKeeper: ZooKeeper exists failed after 4 attempts  
org.apache.zookeeper.KeeperException$ConnectionLossException: KeeperErrorCode = ConnectionLoss for /hbase/hbaseid  
Explanation and Fix: If you run "jps" in terminal you will not find any "HMaster" service running. To fix this run below command 
```
$HBASE_HOME/bin/start-hbase.sh
```
You can use the below command to check if HMaster is running.
```
jps

```

Error 2: Exception in thread "main" org.apache.hadoop.hbase.client.RetriesExhaustedException: Cannot get the location for replica0 of region for Twitter,, in hbase:meta  
Explanation and Fix: You will get this error in eclipse if you run HBase on eclipse when HMaster is not running.   
Go to terminal and run command:
```
$HBASE_HOME/bin/start-hbase.sh
```
