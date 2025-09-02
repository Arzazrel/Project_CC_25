# Project_CC_25
## **DESCRIPTION**  

Project for the cloud computing exam of the AIDE master's degree at the University of Pisa, year 2024-2025.

## **The folder contains:**  

- code: Folder containing all the code used during the project. It contains the Java classes used for Hadoop and also some useful Python programmes.
- documentation: folder containing the PDF documentation for the project and all the images used for the report.
- result: folder containing all the outputs of all the tests performed, organised by type of problem (e.g. word count, CoOccurrence, N-Gram), by methodology used (e.g. Pairs, Stripes) and by configuration (e.g. base, ext_combiner, inmap_combiner).

## **Project Import Guide**  

To set up Hadoop, you can follow the official installation guide, which provides detailed instructions for different deployment options (single-node, pseudo-distributed, and fully distributed modes). 
Please refer to the official documentation here: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

## **Hadoop Installation Guide**  

Note:

questa elencata sotto non vuole essere una guida esaustiva ma una breve guida per fornire velocemente i riferimenti e i link dove poter trovare tutto il materiale apprfondito che servirà. 
Non basatevi solo su quello scritto qui ma guardate nelle pagine ufficiali. L'installazione Hadoop non è costituita da passaggi difficili ma devono essere eseguiti correttamente nel giusto ordine.


## **Jar file generation**  

In this project, Hadoop jobs written in Java were executed. To run these Java programs, it is necessary to create a JAR file that includes the compiled classes and all required dependencies.
To generate the JAR file from a Java class, I used Maven. Below, I will outline the steps needed to create a JAR file from a given class in this project.
0) Install Maven: sudo apt-get install maven 
1) Command to create the folder structure and a pom.xml with the minimum data required. Change groupId and artifactId with your requirements.
	mvn archetype:generate -DgroupId=it.unipi.hadoop -DartifactId=wordcount \ 
                       -DarchetypeArtifactId=maven-archetype-quickstart \ 
                       -DinteractiveMode=false 
				
	Maven will create a folder named after the provided artifactId (in our case, wordcount), including a minimal pom.xml file and a folder structure.
2) Delete/ignore the test folder, as well as the App.java file. We will write our own Java file. Execute these commands:
	cd wordcount 
	rm -rf src/test 
	rm -rf src/main/java/it/unipi/hadoop/App.java 
3) Now you need to update the pom.xml file. Below, you'll see the basic dependency and build packages added to the pom file. These additions were sufficient for this project. 
   Remember to adapt the additions as needed.

	<build> 
    <plugins> 
      <plugin> 
        <artifactId>maven-compiler-plugin</artifactId> 
        <version>3.0</version> 
        <configuration> 
          <source>1.8</source> 
          <target>1.8</target> 
          <encoding>${project.build.sourceEncoding}</encoding> 
        </configuration> 
      </plugin> 
      <plugin> 
        <groupId>org.apache.maven.plugins</groupId> 
        <artifactId>maven-jar-plugin</artifactId> 
        <configuration> 
          <archive> 
            <manifest> 
              <addClasspath>true</addClasspath> 
            </manifest> 
          </archive> 
        </configuration> 
      </plugin> 
    </plugins> 
  </build>
  <dependencies> 
    <dependency> 
      <groupId>org.apache.hadoop</groupId> 
      <artifactId>hadoop-mapreduce-client-jobclient</artifactId> 
      <version>3.1.3</version> 
    </dependency> 
    <dependency> 
      <groupId>org.apache.hadoop</groupId> 
      <artifactId>hadoop-common</artifactId> 
      <version>3.1.3</version> 
    </dependency> 
    <dependency> 
      <groupId>org.apache.hadoop</groupId> 
      <artifactId>hadoop-hdfs-client</artifactId>       
	  <version>3.1.3</version> 
    </dependency> 
    <dependency> 
      <groupId>org.apache.hadoop</groupId> 
      <artifactId>hadoop-mapreduce-client-app</artifactId> 
      <version>3.1.3</version> 
    </dependency> 
  </dependencies> 

4) Now can write the source code of your application with any text editor and put in: nano src/main/java/it/unipi/hadoop/WordCount.java 
5) In the folder containing your pom.xml file, run the following command: mvn clean package 
6) Now can you execute this program with this command: hadoop jar wordcount-1.0-SNAPSHOT.jar it.unipi.hadoop.WordCount input_folder output_folder

Note: Thanks to the Cloud Computing course instructors for providing this helpful material.


## **Project Execution Guide**  

### **General execution guide***
After finishing the steps above, we must execute the following commands:
0) Command to start the NameNode, DataNodes and secondary NameNode, from hadoop-namenode:  start-dfs.sh 
1) Command to start the ResourceManager and NodeManagers, from hadoop-namenode: start-yarn.sh
2) Can run Hadoop code
3) Stop Yarn: stop-yarn.sh
4) Stop NameNode, DataNodes and secondary NameNode: stop-dfs.sh

Note: 
- after start-dfs -> you can access HDFS on a browser on your local machine: hadoop-namenode_IP:9870 (e.g. http://172.16.0.1:9870/)
- after start-yarn -> you can access Resource Manager on a browser on your local machine: hadoop-namenode_IP:8088 (e.g. http://172.16.0.1:8088/)

### **Es. for this project**

To run a job in Hadoop, you first need to configure Hadoop and build the JAR file containing the logic you want to execute.

0) Choose a Java class from the code folder (e.g., WordCount).
1) Create the corresponding JAR file using Maven, as shown above.
2) Start HDFS (command: start-dfs.sh) and YARN (command: start-yarn.sh).
3) Create an input directory in HDFS and populate it with the desired input files.
4) reate an output directory (it must not already contain the results of a previous job).
5) Run the job with the following command (example with WordCount):
hadoop jar wordcount/target/wordcount-1.0-SNAPSHOT.jar it.unipi.hadoop.WordCount /path/to/input /path/to/output 10 false 1

Pay attention to the parameters required by your code. If not specified, default values will be applied.
6) Wait for the job to finish.
7) Once completed, the output will be available in the specified output directory. A statistics file may also be generated with details about executed jobs.
8) When you are done working with Hadoop, stop YARN (command: stop-yarn.sh) and then stop HDFS (command: stop-dfs.sh).

## **Useful HDFS commands** 

Note: These are not all the possible commands, but only a limited number that provide basic functionality and allow you to perform the steps shown in this readme.

il formato generale dei comandi da cmd è il seguente -> hadoop fs <CMD> 

## **Developer's notes**  
  
The work related to the university examination has been done and the project is completed. 
There may be updates or improvements to the project in the future, but nothing is planned for now.

## **Developers:**  

- Alessandro Diana