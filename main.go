package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pinguo-icc/kratos-library/mongo/op"
	"github.com/pinguo-icc/mongodbcli/material"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type dbEntity struct {
	dbName string
	coll   []string
	scope  map[string][]string
}

func (e *dbEntity) dbNames() []string {
	names := []string{}
	for k, v := range e.scope {
		for _, env := range v {
			names = append(names, fmt.Sprintf("%s_%s_%s", k, env, e.dbName))
		}
	}

	return names
}

func (e *dbEntity) run(ctx context.Context, client *mongo.Client, fun func(ctx context.Context, coll *mongo.Collection) error) {
	for _, v := range e.dbNames() {
		log.Printf("======run on db %s ===========\n", v)

		db := client.Database(v)
		for _, c := range e.coll {
			col := db.Collection(c)
			if err := fun(ctx, col); err != nil {
				log.Println(err)
			}
		}

		log.Printf("======run finished =========== \n\n")
	}
}

// setActivityActivityType 添加物料类型类型
func setActivityActivityType(ctx context.Context, coll *mongo.Collection) error {
	filter := primitive.M{"type": primitive.M{"$exists": false}}
	update := primitive.M{"$set": primitive.M{"type": 1}}

	u, err := coll.UpdateMany(ctx, filter, update)
	if err != nil {
		return err
	}

	log.Printf("%s %d doc has been set type field.\n", coll.Name(), u.ModifiedCount)

	return nil
}

// fixActivityActive 修复运营物料上下架状态
func fixActivityActive(ctx context.Context, coll *mongo.Collection) error {
	filter := primitive.M{"active": nil}
	update := primitive.M{"$set": primitive.M{"active": false}}
	u, err := coll.UpdateMany(ctx, filter, update)
	if err != nil {
		return err
	}

	log.Printf("%s %d doc has been set active field.\n", coll.Name(), u.ModifiedCount)

	return nil
}

// setSoftDeleteField 添加软删除
func setSoftDeleteField(ctx context.Context, coll *mongo.Collection) error {
	filter := primitive.M{"isDeleted": primitive.M{"$exists": false}}
	update := primitive.M{"$set": primitive.M{"isDeleted": false}}

	u, err := coll.UpdateMany(ctx, filter, update)
	if err != nil {
		return err
	}

	log.Printf("%s %d doc has been set isDeleted field.\n", coll.Name(), u.ModifiedCount)

	return nil
}

// fixH5ActivityType 修复h5以及H5模版数据类型不一致问题
func fixH5ActivityType(ctx context.Context, coll *mongo.Collection) error {
	filter := primitive.M{
		"pid":       primitive.M{"$exists": false},
		"type":      op.In([]int{2, 3}),
		"isDeleted": false,
	}

	projection := primitive.M{
		"_id":    1,
		"rootID": 1,
		"name":   1,
		"extral": 1,
		"pid":    1,
		"scope":  1,
		"type":   1,
	}

	cur, err := coll.Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return err
	}

	var res []*Activity
	if err := cur.All(ctx, &res); err != nil {
		return err
	}

	for i := range res {
		t := res[i]
		fixH5NodeType(ctx, coll, t)
	}

	return nil
}

func fixH5NodeType(ctx context.Context, coll *mongo.Collection, t *Activity) {
	if t == nil {
		return
	}

	filter := primitive.M{
		"rootID": t.ID,
		"type":   op.Ne(t.Type),
	}

	update := primitive.M{"$set": primitive.M{"type": t.Type}}

	u, err := coll.UpdateMany(ctx, filter, update)
	if err != nil {
		return
	}

	log.Printf("%s %d  nodes has been reset type %d that belown %s .\n", coll.Name(), u.ModifiedCount, t.Type, t.ID.Hex())
}

var execs []dbEntity

func init() {
	execs = []dbEntity{
		{
			dbName: "operational-positions",
			coll:   []string{"activity"},
			scope: map[string][]string{
				"videobeats": {"prod", "operation", "dev", "qa", "pre"},
				"camera360":  {"prod", "operation", "dev", "qa", "pre"},
				"idphoto":    {"prod", "operation", "dev", "qa", "pre"},
				"mix":        {"prod", "operation", "dev", "qa", "pre"},
				"salad":      {"prod", "operation", "dev", "qa", "pre"},
				"inface":     {"prod", "operation", "dev", "qa", "pre"},
				"icc":        {"prod", "operation", "dev", "qa", "pre"},
				// "icc": {"dev"},
			},
		},
	}
}

type option struct {
	MongoDNS string
	Timeout  int
}

func (o *option) validate() error {
	if o.MongoDNS == "" {
		return errors.New("please set mongo connect uri")
	}

	return nil
}

func (o *option) addFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.MongoDNS, "mongo-dns", "", "the mongoDB connect address")
	fs.IntVar(&o.Timeout, "timeout", 1, "the exec timeout setting,default 1 minute")
}

func initOptions(fs *flag.FlagSet, args ...string) (*option, error) {
	o := new(option)
	o.addFlags(fs)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if err := o.validate(); err != nil {
		return nil, err
	}

	return o, nil
}

func exec(ctx context.Context, cli *mongo.Client) {
	for _, v := range execs {
		v.run(ctx, cli, fixH5ActivityType)
	}
}

func main() {
	o, err := initOptions(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:]...)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(o.Timeout)*time.Minute)
	defer cancel()

	dbCli, err := mongo.Connect(ctx, options.Client().ApplyURI(o.MongoDNS))
	if err != nil {
		log.Fatal(err)
	}

	// exec(ctx, dbCli)

	// if err := syncH5Style(ctx, dbCli); err != nil {
	// 	log.Fatal(err)
	// }

	//同步素材
	// if err := material.SyncMaterials(ctx, dbCli); err != nil {
	// 	log.Fatal(err)
	// }

	// 同步分类
	// if err := material.SyncMaterialCategorys(ctx, dbCli); err != nil {
	// 	log.Fatal(err)
	// }

	// //同步素材位置
	// if err := material.SyncMaterialsPosition(ctx, dbCli); err != nil {
	// 	log.Fatal(err)
	// }

	//同步素材位置计划
	if err := material.SyncMaterialsPlan(ctx, dbCli); err != nil {
		log.Fatal(err)
	}
}

func insertDocument(ctx context.Context, client *mongo.Client) error {
	docs := make([]interface{}, 0, 10000)
	for i := int32(0); i < 10000; i++ {
		doc := primitive.M{
			"order_id":     fmt.Sprintf("%d915%d9418012", i, i),
			"refund_price": int64(599) + int64(i),
			"reason":       fmt.Sprintf("没通过电子版驾驶证,%d%d", i, i),
			"attachments":  []string{"https://cloud-image.c360dn.com/FtGBK66QvyYj4i-MHcpL_hnVPP1n"},
			"status":       0,
			"note":         "",
			"created_at":   166330000 + i,
			"audited_at":   0,
			"source":       0,
		}
		docs = append(docs, doc)
	}

	dbName := "idphoto"
	dbCollenction := "photo_rfdlog"
	collenction := client.Database(dbName).Collection(dbCollenction)
	_, err := collenction.InsertMany(ctx, docs)

	return err
}

func syncH5Style(ctx context.Context, client *mongo.Client) error {
	log.Printf("==============run sync start =========== \n")
	scope := map[string][]string{
		"videobeats": {"prod", "operation", "dev", "qa", "pre"},
		"camera360":  {"prod", "operation", "dev", "qa", "pre"},
		"idphoto":    {"prod", "operation", "dev", "qa", "pre"},
		"mix":        {"prod", "operation", "dev", "qa", "pre"},
		"salad":      {"prod", "operation", "dev", "qa", "pre"},
		"inface":     {"prod", "operation", "dev", "qa", "pre"},
		"icc":        {"prod", "operation", "dev", "qa", "pre"},
		//"icc": {"dev"},
	}
	dbActH5Names := make(map[string]string, 0)
	for sp, envs := range scope {
		for _, env := range envs {
			scopEnv := fmt.Sprintf("%s_%s", sp, env)
			actDbName := fmt.Sprintf("%s_%s", scopEnv, "operational-positions")
			h5DbName := fmt.Sprintf("%s_%s", scopEnv, "h5")
			dbActH5Names[actDbName] = h5DbName
		}
	}

	for actDBName, h5DBName := range dbActH5Names {
		log.Printf("==============run sync %s to %s start =========== \n", actDBName, h5DBName)
		actColl := client.Database(actDBName).Collection("activity")
		h5Coll := client.Database(h5DBName).Collection("properties")
		if err := doSyncH5Style(ctx, actColl, h5Coll); err != nil {
			log.Printf("sync style %s to %s error :%v", actDBName, h5DBName, err)
		}

		log.Printf("==============run sync %s to %s end =========== \n", actDBName, h5DBName)
	}

	log.Printf("==============run sync finished =========== \n")

	return nil
}

func doSyncH5Style(ctx context.Context, actColl, h5Coll *mongo.Collection) error {
	filter := primitive.M{
		"type":      op.In([]int{2, 3}),
		"isDeleted": false,
	}

	projection := primitive.M{
		"_id":    1,
		"rootID": 1,
		"name":   1,
		"extral": 1,
		"pid":    1,
		"scope":  1,
		"type":   1,
	}

	cur, err := actColl.Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return err
	}

	var res []*Activity
	if err := cur.All(ctx, &res); err != nil {
		return err
	}

	log.Printf("sync %d activitys start \n", len(res))
	for i := range res {
		t := res[i]
		style, attr := extractStyleAndAttribue(t.Extral.Html5Style)
		actID := t.ID.Hex()
		pro := H5Properties{}
		err := h5Coll.FindOne(ctx, primitive.M{"activityID": actID}).Decode(&pro)
		if err != nil {
			pro.ID = primitive.NewObjectID()
		}

		pro.Attribute = attr
		pro.Style = style
		pro.ActivityID = t.ID.Hex()

		_, err = h5Coll.UpdateByID(context.TODO(), pro.ID, op.Set(pro), options.Update().SetUpsert(true))
		if err != nil {
			log.Printf("%d sync activity %s failed,error: %s \n", i, t.ID.Hex(), err)
		} else {
			log.Printf("%d sync prop %+v success \n", i, pro)
		}
	}

	return nil
}

func extractStyleAndAttribue(extral string) (syle, attr string) {
	// "{\"style\":{\"opacity\":1},\"attribute\":{\"zIndex\":0,\"left\":0,\"borderRadius\":10}}"
	t := struct {
		Style     interface{} `json:"style"`
		Attribute interface{} `json:"attribute"`
	}{}

	err := json.Unmarshal([]byte(extral), &t)
	if err != nil {
		return "", ""
	}

	if t.Style != nil {
		sb, err := json.Marshal(t.Style)
		if err == nil {
			syle = string(sb)
		}
	}

	if t.Attribute != nil {
		ab, err := json.Marshal(t.Attribute)
		if err == nil {
			attr = string(ab)
		}
	}

	return syle, attr
}

type H5Properties struct {
	ID         primitive.ObjectID `bson:"_id"`
	Attribute  string             `bson:"attribute"`
	Style      string             `bson:"style"`
	ActivityID string             `bson:"activityID"` // h5属性与h5物料关联关系的体现
}

type Activity struct {
	ID    primitive.ObjectID `bson:"_id,omitempty"`
	Scope string             `bson:"scope"`
	PID   primitive.ObjectID `bson:"pid,omitempty"`
	// 根节点ID
	// 活动数据一般按根节点查询，且需要查询出指定根节点下的全部后代节点
	// 该字段用作存储根节点，以避免使用递归查询才能构建出活动树
	// * 当该活动自身为根节点时，约定该字段的值与ID相同
	RootID primitive.ObjectID `bson:"rootID"`
	Type   int                `bson:"type"`
	Extral Exteral            `bson:"extral"`
}

type Exteral struct {
	Html5Style string `bson:"html5Style"`
}
