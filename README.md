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
    #prod
    ./mongodbcli --mongo-dns="mongodb://ops:Dpmp34B%3D1a@dds-bp1a2ce765c7df241.mongodb.rds.aliyuncs.com:3717,dds-bp1a2ce765c7df242.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-55928918" --action="syncH5Style"
    #prod - relationship_id
    ./mongodbcli -mongo-dns="mongodb://ops:Dpmp34B%3D1a@dds-bp1a2ce765c7df241.mongodb.rds.aliyuncs.com:3717,dds-bp1a2ce765c7df242.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-55928918" --bmall-mongo-dns="mongodb://10.1.7.126:48110,10.1.8.186:48110,10.1.9.21:48110/bmall?replicaSet=bmallrec_rs1&readPreference=secondaryPreferred" --action="mapOfBmallAndOPS" --timeout=60
    # test
    --mongo-dns="mongodb://192.168.1.233:27017"
```