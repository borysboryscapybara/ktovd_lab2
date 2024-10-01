# ktovd_lab2

pip install pyspark
![alt text](image.png)

```
set SPARK_HOME=C:\path\to\spark
set HADOOP_HOME=C:\path\to\spark\bin
pip install pyspark
pip install psutil
```

```
PS D:\knute\ktovd\lab\lab 2\ktovd_lab2>  & 'd:\knute\ktovd\lab\lab 2\ktovd_lab2\.venv\Scripts\python.exe' 'c:\Users\chere\.vscode\extensions\ms-python.debugpy-2024.10.0-win32-x64\bundled\libs\debugpy\adapter/../..\debugpy\launcher' '55527' '--' 'd:\knute\ktovd\lab\lab 2\ktovd_lab2\part1.py'
 '55527' '--' 'd:\x5cknute\x5cktovd\x5clab\x5clab 2\x5cktovd_lab2\x5cpart1.py' ;b1a6c076-2d0d-4ee6-b7e9-555d0ab4219024/10/01 12:23:32 WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException: java.io.FileNotFoundException: Hadoop home directory C:\hadoop does not exist -see https://wiki.apache.org/hadoop/WindowsProblems
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/10/01 12:23:32 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
+----+---+
|Name|Age|
+----+---+
|John| 30|
| Doe| 25|
|Jane| 28|
+----+---+
```





## Part 1
```
Слова великими літерами: ['APPLE', 'BANANA', 'CHERRY', 'DATE', 'ELDERBERRY', 'FIG', 'GRAPE', 'BANANA', 'APPLE']
Слова, що починаються з 'B': ['banana', 'banana']
Унікальні слова: ['apple', 'banana', 'cherry', 'date', 'elderberry', 'fig', 'grape']
Числа в квадраті: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
Парні числа: [2, 4, 6, 8, 10]
Кількість парних чисел: 5
Сума всіх чисел: 55
Добуток всіх чисел: 3628800
Перший підрахунок: 10
Другий підрахунок: 10
Третій підрахунок: 10
```

## Part 2.1


## Part 2.2 

```
Top 10 IP addresses by request count:
+---------------+-----+ 
|ip             |count| 
+---------------+-----+ 
|192.168.145.92 |23043| 
|192.168.145.107|7521 | 
|192.168.1.255  |7160 | 
|192.168.1.121  |7059 | 
|192.168.1.253  |5820 | 
|10.240.53.6    |5733 | 
|192.168.1.146  |5532 | 
|10.240.53.113  |5402 | 
|192.168.145.30 |5082 | 
|10.240.53.5    |5068 | 
+---------------+-----+ 
only showing top 10 rows


Request counts by status:
+------+------+
|status|count |
+------+------+
|200   |448119|
|206   |264   |
|304   |1583  |
|400   |4     |
|403   |733   |
|404   |1252  |
|500   |118   |
+------+------+
```