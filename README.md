# Redpanda x Java Tutorial
Redpanda tutorial for Vectorized technical documentation.
This project intends to demonstrate how RedPanda can be used alogn with a Java Application.

<h2>Introduction</h2>
Redpanda is a new streaming platform that is fully compatible with Kafka, but making it easier to use, once it does not requires Kafka Zookeeper, JVM and it can even be used with your current code, without changing it.
This document will guide you to properly configure your environment, execute the code and check for the output.


<h2>Requirements</h2>
Following are the tools and applications used to create this tutorial, feel free to use a different IDE or Java version, just be aware that Spring Boot and Kafka versions might behave differently. Also this tutorial was totally implemented on Windows OS, which requires Docker to execute RedPanda.
<ul>  
  <li><a href="https://www.oracle.com/br/java/technologies/javase/jdk11-archive-downloads.html">Java Development Kit 11</a></li>
  <li><a href="https://maven.apache.org/index.html">Apache Maven</a></li>
  <li><a href="https://www.jetbrains.com/idea/">Intellij IDEA</a></li>
</ul> 

<h3>Setup</h3>
If you are having trouble to setup any of the tools above, the following links might help:
<ul>
  <li><a href="https://docs.oracle.com/en/java/javase/11/install/installation-jdk-microsoft-windows-platforms.html#GUID-0DB9580B-1ACA-4C13-8A83-9780BEDF30BB">Java Setup on Windows</a></li>
  <li><a href="https://maven.apache.org/install.html">Apache Maven Installation</a></li>
  <li><a href="https://docs.docker.com/desktop/windows/install/">Docker Setup on Windows</a></li>
  <li><a href="https://vectorized.io/docs/quick-start-windows">Redpanda Windows Quickguide Guide</a></li>
</ul>

<h2>Code execution</h2>
<p dir="auto">First step it to have Redpanda up and running so the queues can be properly create. If you followed Redpanda quickguide then it is possible to only copy, paste and run on your CMD and run the following commands:</p>

<p dir="auto"><code>docker exec -it redpanda-1 ^
   rpk topic delete order_updates --brokers=localhost:9092</code></p>
<p dir="auto"><code>docker exec -it redpanda-1 ^
rpk topic delete pending_orders --brokers=localhost:9092</code></p>
<p dir="auto"><code>docker exec -it redpanda-1 ^
rpk topic delete cancelled_orders --brokers=localhost:9092</code></p>

<p dir="auto"><b>*Be aware that if you have created a cluster with a different name/port or even if are running in different OS, the commands above must be changed to reflect your environment*</b></p>
You can check if the topics were properly created by executing the following command line:
<p dir="auto"><code>docker exec -it redpanda-1 ^
rpk topic list --brokers=localhost:9092</code></p>
  
Next step is to clone the git repo on your local, making sure that you have properly build the project without any errors.
Now you should be able to run the project and check the output. In the logs you might be able to see the messages being sent from the producer:
![alt text](https://user-images.githubusercontent.com/31374207/145735329-71a546ae-ae6e-4a2b-a2ff-54173fc04f11.JPG)
and also check the listeners consume the message based on the topic name:
![alt text](https://user-images.githubusercontent.com/31374207/145735326-ea276732-cfc8-4cb1-a2e3-537bf2976aac.JPG)
<h3>Tips</h3>
If you would like to create different scenarios check the class <a href="https://github.com/marcoprestes/redpanda-tutorial/blob/main/src/main/java/org/vectorized/redpanda/tutorial/RedpandaTutorialApplication.java">RedpandaTutorialApplication</a> or add different test cases.
  
