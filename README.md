# BigData23Assignment3
## Assignment 3: Spark Streaming with text
For this assignment, you will construct a predictive model based on a streaming textual data source using Spark Streaming. The data you'll work with consists of edits made on Wikipedia. You'll use the edit date to predict whether the edit was either "safe", "unsafe" or "vandal".

Setting up Spark

Since the data set we'll work with is still relatively small, you will (luckily) not need a cluster of machines, but can simply run Spark locally on your machine (and save data locally as well). Here's how you make sure you have a local Spark set up and ready to go:

First, download the ZIP file from https://drive.google.com/open?id=1cTvZYjVpbmPL0bqMRn--1xJsL6Xj36EP and extract it somewhere, e.g. on your Desktop. This ZIP file contains a portable Spark installation with Java and necessary tooling included. It includes the latest stable Spark version at this time (2.4.6).

Second, make sure you have a Python 3 Anaconda distribution installed on your system. (Python 3 and Jupyter Notebook need to be available on your system.)

Windows users: to start Spark, double-click "letsgo-win.bat". If all goes well, a Jupyter notebook should open up with a connection to Spark being established and ready to go. If the launcher fails to find Python, it will provide you with an error message. Upon starting, Java might require access to your Windows firewall, which you can safely accept.

Mac users: if you're on Mac, open up a Terminal window and navigate to where you've unzipped the file and run "letsgo-mac.sh", e.g.:

cd /Users/seppe/Desktop/spark
./letsgo-mac.sh

Again, do make sure you have installed Python 3 Anaconda first. If Mac complains about the file not being executable, you might first have to enter the following command to make it executable:

chmod +x ./letsgo-mac.sh

You might also get a window popup asking if you want to install XCode. You can ignore this, as you don't need it. If all goes well, a Jupyter notebook should open up with a connection to Spark being established.
If you encounter issues, check the FAQ below first -- otherwise, feel free to e-mail me.

Example notebooks

Once you have Jupyter notebook open, feel free to explore the example notebooks under "notebooks":

spark_example.py.ipynb: A simple Spark example (calculating pi) to check if your installation is working
spark_streaming_example.py.ipynb: A simple Spark Streaming example that prints out the data you'll work with
spark_streaming_example_saving.py.ipynb: A simple Spark Streaming example that saves the data
spark_streaming_example_predicting.py.ipynb: A very naive prediction approach
spark_structured_streaming_example.py.ipynb: An example using Spark Structured Streaming
The last part of the recording for "31-03" will show you what these do
Assignment

Using Spark, your task for this assignment is as follows:

Construct a historical data set using the provided stream
Important: get started with this as soon as possible, as there is a large class imbalance. We will discuss text mining in more detail later on, but you can already start gathering your data
Make sure to set up Spark first using the instructions posted above if you haven't done so already
The streaming server is running at "seppe.net:7778", as shown in the example notebooks
Construct a predictive model to predict the outcome ("safe", "unsafe", "vandal") of the edit based on the features available
The stream is text-based with each line containing one edit (instance) formatted as a JSON dictionary with the following keys (features): "comment" (comment entered together with the edit), "text_old" and "text_new" (text before and after the edit, formatted as WikiMedia text), "title_page" and "url_page" (title and URL of the edited page) and "label" (the target)
You can use extra data and libraries if you want, but this is not required
However, it's probably going to be very helpful if you figure out a way to create a "diff" between the text before and after the edit. Take a look at the example notebooks for some pointers. Other smart features can be created as well
You are encouraged to build your model using spark.ml (MLlib), but you can use scikit-learn as a fallback
Use your model to show you can make predictions as the stream comes in
I.e. show that you can connect to the data source, preprocess/featurize incoming edits, have your model predict the label, and show it, similar to "spark_streaming_example_predicting.py.ipynb" (but hopefully using a smarter, real predictive model)
This means that you'll need to look for a way to save and load your trained model
The third part of your lab report should contain:

Overview of the steps above, the source code of your programs, as well as the output after running them
Feel free to include screen shots or info on encountered challenges and how you dealt with them
Even if your solution is not fully working or not working correctly, you can still receive marks for this assignment if you show what you tried and how you'd need to improve your end result (i.e. when you can prove a conceptual understanding of the problem and solution)
Further remarks

Get started with setting up Spark and fetching edits as quickly as possible. If you encounter troubles getting Spark to run, do let me know
The rate at which edits come in is pretty quick -- you obviously don't need all of them, but make sure to have enough (and have some for all classes) to train your model
The data stream is line delimited with every line contain a review in JSON format, but can be easily converted to a DataFrame (and RDD). The example notebooks give some ideas on how to do so
You can use both Spark Streaming or Spark Structured Streaming. The former is most likely easier to work with
The focus of this assignment is on getting the full pipeline as outlined above constructed, not on getting spectacularly high accuracies, though TF-IDF, ... might be interesting to apply
Preferably, your predictive model needs to be build using MLlib (so read documentation and tutorials). In case you encounter trouble, you can use scikit-learn as well to still perform the "deployment" stage. As stated above, other libraries can be used as well, as long as you can show that your model can provide predictions in real-time
Let me know in case the streaming server would crash… simply with an e-mail (don't hesitate!)
As discussed in class: you're free to schedule your planning and division of work. You do not hand in each assignment separately, but hand in your completed lab report containing all four assignments on Sunday May 31st.

FAQ
"letsgo-win.bat" doesn't start the notebook -- it says I have spaces in my path

Try again by moving the installation to a directory without spaces.

"letsgo-win.bat" doesn't start the notebook -- it says it can't find Python

call C:\Users\%username%\Anaconda3\Scripts\activate.bat C:\Users\%username%\Anaconda3

To match the correct Anaconda installation on your system and base path on your system.

"letsgo-win.bat" doesn't start the notebook -- the command line window just disappears

Most likely, the script has trouble to find your Anaconda installation. Open an "Anaconda Prompt" and navigate to the directory where you've unpacked Spark, e.g. with cd c:\users\yourname\Desktop\spark. Then start letsgo-win.bat from there

It seems I receive duplicated data when restarting my Jupyter notebook

The server keeps a limited history (about 10 minutes) of edits, so it is possible you see the same edits coming in if you restart your client. Make sure to remove those duplicates before training your model.

I've looked up an edit on Wikipedia and it's older than this exact time

That can happen as their might be some time between extraction of the edit and sending it to you.

I can't save the stream... everything else seems fine

Make sure you're calling the "saveAsTextfiles" function with "file:///" prepended to the path:

lines.saveAsTextFiles("file:///C:/...")

Also make sure that the folder where you want to save the files exist. Note that the "saveAsTextfiles" method expects a directory name as the argument. It will automatically create a folder for each mini-batch of data.

Can I prevent the "saveAsTextfiles" function from creating so many directories and files?

You can first repartition the RDD to one partition before saving it:

lines.repartition(1).saveAsTextFiles("file:///C:/...")

To prevent multiple directories, change the trigger time to e.g.

ssc = StreamingContext(sc, 60)

Though this will still create multiple directories. Setting the trigger interval higher is not really recommended, as you wouldn't want to lose data in case something goes wrong.

So if I still end up with multiple directories, how do I read them in?

It's pretty easy to loop over subdirectories in Python. This can be easily found on Google.

Another tip: alternatively, the "sc.textFile" command is pretty smart and can parse through multiple files in one go.

Is it normal all my folders only contain "_SUCCESS" files but no actual data files?

That depends. A "_SUCCESS" file indicates that the mini-batch was saved correctly. "part-*" files contain the actual data. And files ending with ".crc" contain a checksum which you can ignore. It's normal if not all of your folders contain "part-*" data, as it might be that no edits were received in that time frame.

However, if none of your folders are having data and you've been trying to run the code for some time, something else has gone wrong (e.g. an Internet hiccup). Try the "spark_streaming_example.py.ipynb" notebook to verify whether you're at least receiving data at all.

The above happens often to me: I receive data for a few hours and then Spark gets stuck. Is there a way how I can solve this without having to baby sit Spark?

You can try the following: stop Spark completely and navigate to "spark-2.4.5-bin-hadoop2.7/conf". Create a new file named "spark-defaults.conf" (not "spark-defaults.conf.txt" (!) a "spark-defaults.conf.template" file should already be present). In that file, enter the line:

spark.speculation = true

Now start Spark back up again. Enabling speculation will allow Spark to cancel and retry tasks that take too long and get stuck.

Is there a way how I can monitor Spark?

Yes, go to  http://127.0.0.1:4040/ in your browser while Spark is running and you'll get access to a monitoring dashboard. Under the "Environment" tab, you should be able to find a "spark.speculation" entry for instance w.r.t. the question above. Under "Jobs", "Stage", and "Streaming", you can get more info on how things are going.

I'm trying to convert my saved files to a DataFrame, but Spark complains for some files?

Data is always messy, especially the ones provided by this instructor. Make sure you can handle badly formatted lines and discard them.

My stream crashes after a while with an "RDD is empty" error

Make sure you're checking for empty RDDs, e.g.

if rdd.isEmpty():
    return

I've managed to create a model. When I try to apply it on the stream, Spark crashes with a Hive / Derby error, e.g. when I try to .load() my model(s) or once the first RDD arrives

Check the example notebooks on how to load in your model in "globals()" once.

When I call "ssc_t.stop()"... Spark never seems to stop the stream

You can try changing "stopGraceFully=True" to "False". Even then, Spark might not want to stop its stream processing pipeline in case you're doing a lot with the incoming data, preventing Spark from cleaning up. Try decreasing the trigger time, or simply restart the Jupyter kernel to start over.

Spark complains that only one StreamingContext can be active at a time (and other general "it doesn't work anymore" questions)

In case of trouble, a good idea is always to (save and) close all running notebooks and start again fresh.

I can't access Spark when starting Jupyter manually

That's right. The script provided here links PySpark to Jupyter so the "sc" and "spark" variables will be created for you. It's hence best to use the provided "letsgo-*" scripts.

Can I use R?

There are two main Spark R packages being maintained right now: SparkR (the official one) and sparklyr (from the folks at RStudio and fits better with the tidyverse). Both are fine to use, but you'll have to do some setting up in order so R can find your Spark installation. The example below assumes you have the "SparkR" library installed:

### Set up environment variables: do this before loading the the library
### Otherwise, R will attempt to download spark on its own
### Make sure to adjust the paths below to match your system
### HADOOP_HOME is only required on Windows, remove this line for Max/Linux
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "C:\\Users\\Seppe\\Desktop\\spark\\spark-2.4.0-bin-hadoop2.7\\")
  Sys.setenv(HADOOP_HOME = "C:\\Users\\Seppe\\Desktop\\spark\\winutils\\")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

### Set up the Spark Context:
sparkR.session(master = "local[*]")

### Read a structured streaming data frame:
edits <- read.stream("socket", host = "seppe.net", port = 7778)

### Provide a sink, using "console" will not show anything on your main R console
### So use memory with a queryName instead:
query <- write.stream(edits, "memory", queryName="edits")

### Your operations:
head(sql("SELECT * FROM edits"))

# Once you are finished, stop the query:
stopQuery(query)

However, I'd strongly recommend using Python.
