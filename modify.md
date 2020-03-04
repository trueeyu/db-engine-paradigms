```
q11: select count(*) from lineorder;
q12: select count(*) from lineorder where lo_orderdate > 19920101;
q13: select sum(lo_revenue) from lineorder;

q21: select sum(lo_revenue), sum(lo_extendedprice), sum(lo_ordtotalprice) from lineorder;
q22: select lo_shopmode, sum(lo_revenue) from lineorder group by lo_shopmode;
q23: select lo_shopmode, lo_orderdate, sum(lo_revenue) from lineorder group by lo_shopmode,lo_orderdate;
```

其中 q23 实现的有问题，但是还没查到问题原因
