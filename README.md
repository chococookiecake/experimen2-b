# 任务⼆：星期交易量统计

## 设计思路
  第一个mapreduce任务的mapper：输入：日期、资金流入、资金流出； 输出：星期几、资金流入、资金流出  
  第一个mapreduce任务的reducer：输入：星期几、（资金流入1、资金流出1）、（资金流入2、资金流出2）、...  ； 输出：星期几、平均流入、平均流出  
  
  而第二个mapreduce任务中mapper的输出将平均流入作为key，用mapreduce框架中的shuffle&sort阶段自动地进行排序，并定义了降序的比较方法  

## 运行结果  

Tuesday	263582058.87,191769144.62  
Monday	260305810,217463865.49  
Wednesday	254162607.84,194639446.51  
Thursday	236425594.03,176466674.89  
Friday	199407923.07,166467960.2  
Sunday	155914551.93,132427205.07  
Saturday	148088068.3,112868942.08  

## web页面截图  
<img width="415" alt="image" src="https://github.com/user-attachments/assets/9ec22a79-ee45-485d-ad81-93f96c62a898">  


## 可能的改进之处
不需要用第二个mapreduce任务来排序，因为一共就七条数据，不必大动干戈。  
