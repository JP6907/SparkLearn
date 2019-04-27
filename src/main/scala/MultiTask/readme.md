 ## 一个Executor的多个task的多线程问题
 
 案例中，使用dateFormat去转化数据格式
 ```java
dateFormat.parse(time).getTime
```
由于这个方法是线程不安全的，因此一个Executor中的多个task同时进行计算就会出现问题

解决：
- 加锁
- 使用线程安全的方法 FastDateFormat