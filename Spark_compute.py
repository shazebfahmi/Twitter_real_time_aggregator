import json
import findspark
import urllib
import urllib.request
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os.path
from os import path
import ast
total_count_downloaded = 0

findspark.init('/usr/lib/spark-3.0.1-bin-hadoop2.7')

if os.path.isdir('./dest_folder') != True:
    try:
        os.mkdir('./dest_folder')
        print("Folder 'dest_folder' created successfully...!!!")
    except Exception as e:
        print(e)

def saveImage(d,path):
    url = d['photo_link']
    url_2 = url.split('/')[4]
    suffix = url_2.split('.')[-1]
    if suffix == 'jpg':
        path = os.path.join(path,'jpg_images')
        if os.path.isdir(path) != True:
            try:
                os.mkdir(path)
                print("Folder {} created successfully...!!!".format(path))
            except Exception as e:
                print(e)

    elif suffix == 'png':
        path = os.path.join(path,'png_images')
        if os.path.isdir(path) != True:
            try:
                os.mkdir(path)
                print("Folder {} created successfully...!!!".format(path))
            except Exception as e:
                print(e)

    full_path = os.path.join(path , d['timestamp_ms'] +'.'+ suffix )

    try:
        urllib.request.urlretrieve(url,full_path)
        print('Image downloaded and saved as : ',full_path,end = '\n\n\n\n\n\n')
    except Exception as e:
        print(e)

def saveVideo(d,path):
    url  =  d['video_link']
    if url.find('mp4') != -1:
        full_path = os.path.join(path, d['timestamp_ms'] + '.mp4')
        try:
            urllib.request.urlretrieve(url,full_path)
            print('Video downloaded and saved as : ',full_path,end = '\n\n\n\n\n\n')
        except Exception as e:
            print(e)

def operation(rdd):
    print('RDD is : ',rdd)
    rdd.foreach(lambda x : accum.add(1) )
    lst = rdd.collect()
    print('---------------------\n\nlist elements in RDD are below : \n')
    for i in lst:
        d = ast.literal_eval(i)
        if 'photo_link' in d.keys():
            if os.path.isdir('./dest_folder/images') != True:
                try:
                    os.mkdir('./dest_folder/images')
                    print("Folder 'dest_folder/images', to store Images created successfully...!!!")
                except Exception as e:
                    print(e)
            saveImage(  d , './dest_folder/images' )

        elif 'video_link' in d.keys():
            if os.path.isdir('./dest_folder/videos') != True:
                try:
                    os.mkdir('./dest_folder/videos')
                    print("Folder 'dest_folder/videos', to store Videos created successfully...!!!")
                except Exception as e:
                    print(e)
            saveVideo(  d , './dest_folder/videos' )

    global total_count_downloaded
    total_count_downloaded = accum.value
    print('Global var inside operation function value: ', total_count_downloaded )
    f = open('Total_downloaded_count.txt','w')
    f.write('Total number of Images and Videos Downloaded are : ')
    f.write(str(total_count_downloaded))

sc = SparkContext(master="local[*]",appName="streaming")
sc.setLogLevel("ERROR")
accum = sc.accumulator(0)

ssc = StreamingContext(sc, 5)
ssc.checkpoint('Checkpoint_Ref_Folder')

socket_stream = ssc.socketTextStream("localhost",5599)
	
lines = socket_stream.window(10,10)
lines.foreachRDD(operation)

ssc.start()
ssc.awaitTermination()








'''words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
print('Final transformation : ', wordCounts)
wordCounts.pprint()'''

#lines.pprint()

#counts = socket_stream.flatMap(lambda line: line.split(","))
#words=counts.filter(lambda text:text.endswith(('.jpg','.png')))

#lines.foreachRDD(process)
