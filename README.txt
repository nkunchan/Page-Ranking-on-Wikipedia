Name : Nikita KUnchanwar
Email id: nkunchan@uncc.edu


#######Assignment3  Readme file######

#####java files#####
1. PageRank.java



Execution Instructions:

** Steps to run PageR.java

1. Compile PageR.java file using below command-
command: javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* PageRank.java -d build -Xlint


2. Create jar file using below command

Command: jar -cvf PageR.java -C build/ .

3. Execute below command with input path and output path as arguments in below format

Command: hadoop jar PageRank.jar org.myorg1.PageRank /user/cloudera/input_graph /user/cloudera/PageRank/output0 

Note: In output path, PageRank/output0 should be given as last extension as shown above, bcoz iterations in the code uses it as input and creates further files like output1, output2 etc




#####output files#####
1. simple_outupt_100.txt
2. micro_output_100.txt



