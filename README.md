# mysql_binlog_stat
MySQL表信息统计小工具

##### 基本使用

```
[root@centos7 tmp]# python mysql_binlog_stat.py --help
usage: mysql_binlog_stat.py [-h] [--host HOST] [--port PORT]
                            [--username USERNAME] [--password PASSWORD]
                            [--log-file binlog-file-name]
                            [--log-pos binlog-file-pos]
                            [--server-id server-id] [--slave-uuid slave-uuid]
                            [--blocking False/True] [--start-time start-time]
                            [--sorted-by insert/update/delete]

Description: The script parse MySQL binlog and statistic column.

optional arguments:
  -h, --help            show this help message and exit
  --host HOST           Connect MySQL host
  --port PORT           Connect MySQL port
  --username USERNAME   Connect MySQL username
  --password PASSWORD   Connect MySQL password
  --log-file binlog-file-name
                        Specify a binlog name
  --log-pos binlog-file-pos
                        Specify a binlog file pos
  --server-id server-id
                        Specify a slave server server-id
  --slave-uuid slave-uuid
                        Specify a slave server uuid
  --blocking False/True
                        Specify is bloking and parse, default False
  --start-time start-time
                        Specify is start parse timestamp, default None,
                        example: 2016-11-01 00:00:00
  --sorted-by insert/update/delete
                        Specify show statistic sort by, default: insert
```
**主要参数介绍:**

--log-file: binlog 文件名称

--log-pos: binlog 文件位置(从哪个位置开始解析)

--blocking: 是否需要使用阻塞的方式进行解析始终为 False 就好(默认就是False)

--start-time: 从什么时间开始解析

--sorted-by: 展示的结果通过什么来排序, 默认是通过 insert 的行数的多少降序排列, 设置的值有 insert/update/delete


##### 解析binlog并统计

```
root@(none) 09:17:12>show binary logs;
+------------------+-----------+
| Log_name         | File_size |
+------------------+-----------+
| mysql-bin.000012 | 437066170 |
| mysql-bin.000013 | 536884582 |
| mysql-bin.000014 | 537032563 |
| mysql-bin.000015 | 536950457 |
| mysql-bin.000016 |  87791004 |
| mysql-bin.000017 |       143 |
| mysql-bin.000018 |       143 |
| mysql-bin.000019 |       143 |
| mysql-bin.000020 |       143 |
| mysql-bin.000021 |      1426 |
+------------------+-----------+
10 rows in set (0.01 sec)

[root@centos7 tmp]# time python mysql_binlog_stat.py --log-file=mysql-bin.000012 --log-pos=120 --username=root --password=root --sorted-by='insert' 
[
    {
        "app_db.business_item_sku_detail": {
            "row_insert_count": {
                "market_price": 273453,
                "sku_id": 273453,
                "weight": 273453,
                "sku_info": 273453,
                "created": 273453,
                "pre_sale_stock": 273453,
                "price": 273453,
                "sku_name": 273453,
                "limit_sale_time": 273453,
                "sku_no": 273453,
                "limit_sale_num": 273453,
                "business_item_id": 273453,
                "channel_sku_lowest_price": 273453,
                "tmall_shop_id": 273453,
                "guid": 273453,
                "pic_url": 273453,
                "stock": 273453
            },
            "table_dml_count": {
                "insert": 273453,
                "update": 0,
                "delete": 0
            },
            "row_update_count": {}
        }
    },
    {
        "app_db.business_item_sku_property": {
            "row_insert_count": {
                "sku_id": 273112,
                "created": 273112,
                "property_value_id": 273112,
                "business_item_id": 273112,
                "record_id": 273112,
                "property_id": 273112
            },
            "table_dml_count": {
                "insert": 273112,
                "update": 0,
                "delete": 0
            },
            "row_update_count": {}
        }
    },
    {
        "app_db.business_item_pic": {
            "row_insert_count": {
                "created": 270993,
                "business_item_id": 270993,
                "pic_id": 270993,
                "pic_no": 270993,
                "tmall_shop_id": 270993,
                "pic_url": 270993
            },
            "table_dml_count": {
                "insert": 270993,
                "update": 0,
                "delete": 0
            },
            "row_update_count": {}
        }
    },
    {
        "app_db.business_item": {
            "row_insert_count": {
                "guide_commission": 264803,
                "commission_type": 264803,
                "pstatus": 264803,
                "num_iid": 264803,
                "limit_sale_time": 264803,
                "sell_point": 264803,
                "abbreviation": 264803,
                "distribution_time": 264803,
                "view_num": 264803,
                "tariff_rate": 264803,
                "tmall_shop_id": 264803,
                "is_pre_sale": 264803,
                "pic_url": 264803,
                "pre_sale_begin_time": 264803,
                "business_item_id": 264803,
                "sale_tax": 264803,
                "guid": 264803,
                "recommend_time": 264803,
                "is_top_newgood": 264803,
                "is_delete": 264803,
                "is_open_item_property": 264803,
                "mstatus": 264803,
                "pre_sale_end_time": 264803,
                "top_time": 264803,
                "country_id": 264803,
                "vir_sales_num": 264803,
                "content": 264803,
                "commission": 264803,
                "wholesale_sales_num": 264803,
                "is_associated_type": 264803,
                "recommend": 264803,
                "is_cross_border": 264803,
                "sales_num": 264803,
                "custom_discount_type": 264803,
                "use_item_type_tax_rate": 264803,
                "one_type_id": 264803,
                "new_good_time": 264803,
                "ship_time": 264803,
                "value_add_tax": 264803,
                "new_good_words": 264803,
                "top_time_newgood": 264803,
                "bar_code": 264803,
                "price": 264803,
                "business_no": 264803,
                "limit_sale_num": 264803,
                "is_top_hot_sell": 264803,
                "discount_type": 264803,
                "is_top": 264803,
                "tax_rate": 264803,
                "hot_sell_time": 264803,
                "is_taobao_item": 264803,
                "business_item_brand_id": 264803,
                "logistics_costs": 264803,
                "business_type": 264803,
                "guide_commission_type": 264803,
                "is_top_recommend": 264803,
                "created": 264803,
                "pre_sale_stock": 264803,
                "title": 264803,
                "two_type_id": 264803,
                "new_good_flag": 264803,
                "custom_clear_type": 264803,
                "top_time_recommend": 264803,
                "store_commission_type": 264803,
                "store_commission": 264803,
                "is_hot_sell": 264803,
                "like_num": 264803,
                "distribution": 264803,
                "stock": 264803,
                "channel_item_lowest_price": 264803,
                "top_time_hot_sell": 264803
            },
            "table_dml_count": {
                "insert": 264803,
                "update": 0,
                "delete": 0
            },
            "row_update_count": {}
        }
    },
    {
        "test.t_binlog_event": {
            "row_insert_count": {
                "auto_id": 5926,
                "dml_sql": 5926,
                "dml_start_time": 5926,
                "dml_end_time": 5926,
                "start_log_pos": 5926,
                "db_name": 5926,
                "binlog_name": 5926,
                "undo_sql": 5926,
                "table_name": 5926,
                "end_log_pos": 5926
            },
            "table_dml_count": {
                "insert": 5926,
                "update": 0,
                "delete": 4017
            },
            "row_update_count": {}
        }
    },
    {
        "test.ord_order": {
            "row_insert_count": {
                "order_id": 184,
                "pay_type": 181,
                "amount": 184,
                "create_time": 184,
                "serial_num": 181
            },
            "table_dml_count": {
                "insert": 184,
                "update": 0,
                "delete": 0
            },
            "row_update_count": {}
        }
    },
    {
        "test.t1": {
            "row_insert_count": {
                "id": 7,
                "name": 7
            },
            "table_dml_count": {
                "insert": 7,
                "update": 2,
                "delete": 2
            },
            "row_update_count": {
                "name": 2
            }
        }
    },
    {
        "test.area": {
            "row_insert_count": {},
            "table_dml_count": {
                "insert": 0,
                "update": 0,
                "delete": 0
            },
            "row_update_count": {}
        }
    }
]

real    5m42.982s
user    5m26.080s
sys     0m8.958s
```

> **Tips:** 当程序在运行的时候使用 `kill pid` 也会打印出相关信息, 不过排序是按默认的`insert`排序,使用`Ctrl + c`也会实现相关效果
