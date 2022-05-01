[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-f059dc9a6f8d3a56e377f745f24479a46679e63a5d9fe6f495e02850cd0d8118.svg)](https://classroom.github.com/online_ide?assignment_repo_id=6999006&assignment_repo_type=AssignmentRepo)
# CS6240-project
Spring 22

**Project Overview**
--------------------
Implemented Decision tree algorithm from scratch for training and testing on distributed systems [AWS] using MapReduce framework. Dataset used for training is about 1.35GB and 600MB for testing. Used 'Reduction by variance' technique to decide the best splits for decision tree. 

Code author
-----------
Dheeraj Gadwala, Savitha Munirajaiah, Shreya Singh

Template author
-----------
Joe Sackett

Installation
------------
These components are installed:
- JDK 1.8
- Hadoop 2.9.1
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases: export JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home"<br>
   export HADOOP_HOME="/Users/shreyasingh/hadoop2.10/hadoop-2.10.1"<br>
   export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop<br>
   export PYENV_ROOT="$HOME/.pyenv"<br>
   export PATH=$JAVA_HOME/bin:/Users/shreyasingh/apache-maven3.8.4/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin:$PYENV_ROOT/bin:/opt/homebrew/bin:$PATH

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:<br>
   export JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home"

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
   Sufficient for standalone: hadoop.root, jar.name, local.input
   Other defaults acceptable for running standalone.
5) Standalone Hadoop:
   make switch-standalone		-- set standalone Hadoop environment (execute once)
   make local

6) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)

   make upload-input-aws-TrainTest		-- only before first execution<br>
   make aws-train					-- check for successful execution with web interface (aws.amazon.com)<br>
   make download-output-aws			-- after successful execution download the split file<br>
   run ReadSplitsBeforeBroadcast locally<br>
   make upload-input-aws-broadcastSplits --upload broadcasted split files <br>
   for testing:change to DecisionTreeTest in main<br>
   make aws-test                 --run jar for testing<br>
   
**Usage**
---------
1. Configure training dataset and run the DecisionTree.java to start training the model.
2. The trained decision tree model is stored in splits folder.
3. Locally run ReadSplitsBeforeBroadcast.java class.
4. Output from step 3 with the test dataset is used to run the DecisionTreeTest.java job.
5. The Accuracy of the model is printed in system.out.
