### mongodbcli 
用于给指定数据库制定表添加软删除字段。

#### 使用
```shell
    go build 
    ./mongodbcli --mongo-dns="mongodb://10.1.12.22:27017"
    #dev 
    ./mongodbcli --mongo-dns="mongodb://a985645edaa0f43f8a2a35e4e541ef5e-1339485449.cn-northwest-1.elb.amazonaws.com.cn:27017/?readPreference=primary&directConnection=true&ssl=false"
    #qa
    ./mongodbcli --mongo-dns="mongodb://root:Oldgirl20220402@dds-bp18678321e991f41210-pub.mongodb.rds.aliyuncs.com:3717,dds-bp18678321e991f42162-pub.mongodb.rds.aliyuncs.com:3717/?authSource=admin&replicaSet=mgset-59911852"
    # test
    --mongo-dns="mongodb://192.168.1.233:27017"
```