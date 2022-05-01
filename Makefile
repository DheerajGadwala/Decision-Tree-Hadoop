# Makefile for Hadoop MapReduce WordCount demo project.

# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/usr/local/hadoop-2.9.1
jar.name=mr-demo-1.0.jar# the jar name for your project
jar.path=target/${jar.name}
job.name=decisiontree.DecisionTree
local.trainInput=input/train
local.testInput=input/test
local.trainSample=intermediary/trainSample
local.testSample=intermediary/testSample
local.levelData=intermediary/levelData
local.treeLevel=intermediary/treeLevel
local.splits=intermediary/splits
local.broadcastSplits=intermediary/broadcastSplits
local.leafNodes=intermediary/leafNodes
local.varianceCap=0.08
local.maxDepth=12
local.sampleSize=100
local.intermediary=intermediary
# Pseudo-Cluster Execution
hdfs.user.name=joe
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.17.0# previous version 5.17.0 | edit this with current EMR version.
aws.region=us-east-1
aws.bucket.name=savdt1405# edit this with your bucket name (from S3) for the project.
aws.subnet.id=subnet-6356553a# no need to edit this
aws.trainInput=train
aws.testInput=test
aws.trainSample=trainSample
aws.testSample=testSample
aws.levelData=levelData
aws.treeLevel=treeLevel
aws.splits=splits
aws.broadcastSplits=broadcastSplits
aws.leafNodes=leafNodes
aws.varianceCap=0.08
aws.maxDepth=13
aws.sampleSize=2
aws.log.dir=logMR
aws.num.nodes=8#5 # 8 is a big cluster might not be available for free tier
aws.instance.type=m4.xlarge#m4.xlarge
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.intermediary}*

# Runs standalone  --- runs on local system
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
local: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${local.trainInput} ${local.testInput} ${local.trainSample} ${local.testSample} ${local.levelData} ${local.treeLevel} ${local.splits} ${local.broadcastSplits} ${local.leafNodes} ${local.varianceCap} ${local.maxDepth} ${local.sampleSize}

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs:
	${hadoop.root}/sbin/stop-dfs.sh

# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs: clean-local-output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.testInput} s3://${aws.bucket.name}/${aws.testInput}
	aws s3 sync ${local.trainInput} s3://${aws.bucket.name}/${aws.trainInput}

# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.trainSample}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.testSample}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.levelData}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.treeLevel}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.splits}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.broadcastSplits}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.leafNodes}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
aws: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Decision Tree" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.trainInput}","s3://${aws.bucket.name}/${aws.testInput}","s3://${aws.bucket.name}/${aws.trainSample}","s3://${aws.bucket.name}/${aws.testSample}","s3://${aws.bucket.name}/${aws.levelData}","s3://${aws.bucket.name}/${aws.treeLevel}","s3://${aws.bucket.name}/${aws.splits}","s3://${aws.bucket.name}/${aws.broadcastSplits}","s3://${aws.bucket.name}/${aws.leafNodes}","${aws.varianceCap}","${aws.maxDepth}","${aws.sampleSize}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.splits} ${local.splits}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f MR-Demo.tar.gz
	rm -f MR-Demo.zip
	rm -rf build
	mkdir -p build/deliv/MR-Demo
	cp -r src build/deliv/MR-Demo
	cp -r config build/deliv/MR-Demo
	cp -r input build/deliv/MR-Demo
	cp pom.xml build/deliv/MR-Demo
	cp Makefile build/deliv/MR-Demo
	cp README.txt build/deliv/MR-Demo
	tar -czf MR-Demo.tar.gz -C build/deliv MR-Demo
	cd build/deliv && zip -rq ../../MR-Demo.zip MR-Demo