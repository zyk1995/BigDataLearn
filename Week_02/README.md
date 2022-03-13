# 工作目录 /home/student5/jonas
# 先上传数据文件
$ hdfs dfs -mkdir /user/jonas
$ hdfs dfs -put HTTP_20130313143750.dat /user/jonas/
# 检查文件
$ hdfs dfs -ls /user/jonas
Found 1 items
-rw-r-----   2 student5 hadoop       2229 2022-03-12 15:38 /user/jonas/HTTP_20130313143750.dat
# 清除前次作业输出
$ hdfs dfs -rm -r /user/jonas/output
$ rm -rf output
# 执行作业
$ hadoop jar $HOME/jonas/trafficcount.jar com.example.trafficcount.TrafficCount
# 下载输出
$ hdfs dfs -get /user/jonas/output
# 遍历显示输出内容
$ find output -name "part-*" | while read line; do
cat $line
done
13560439658     2034    5892
13925057413     11058   48243
15013685858     3659    3538
15920133257     3156    2936
15989002119     1938    180
13660577991     6960    690
13760778710     120     120
13922314466     3008    3720
18320173382     9531    2412
13480253104     180     180
13926251106     240     0
13726238888     2481    24681
13560436666     1116    954
13726230503     2481    24681
13926435656     132     1512
18211575961     1527    2106
13502468823     7335    110349
13602846565     1938    2910
84138413        4116    1432
13719199419     240     0
13826544101     264     0