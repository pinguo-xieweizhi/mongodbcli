package material

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pinguo-icc/field-definitions/api"
	"github.com/pinguo-icc/go-base/v2/event"
	"github.com/pinguo-icc/go-lib/v2/dao"
	"github.com/pinguo-icc/kratos-library/mongo/op"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"k8s.io/apimachinery/pkg/util/sets"
)

var (
	scope = map[string][]string{
		"videobeats": {"prod", "operation", "dev", "qa", "pre"},
		"camera360":  {"prod", "operation", "dev", "qa", "pre"},
		"idphoto":    {"prod", "operation", "dev", "qa", "pre"},
		"mix":        {"prod", "operation", "dev", "qa", "pre"},
		"salad":      {"prod", "operation", "dev", "qa", "pre"},
		"inface":     {"prod", "operation", "dev", "qa", "pre"},
		"icc":        {"prod", "operation", "dev", "qa", "pre"},
		"april":      {"prod", "operation", "dev", "qa", "pre"},
		// "videobeats": {"operation"},
	}
	dbOldNewMap             = make(map[string]string, 0)
	dbOldMFieldMap          = make(map[string]string, 0)
	dbOldPosNewMap          = make(map[string]string, 0)
	categorySyncErr         = make([]*SyncRecoder, 0, 100)
	materialPositionSyncErr = make([]*SyncRecoder, 0, 100)
	planPositionSyncErr     = make([]*SyncRecoder, 0, 100)
	wg                      = sync.WaitGroup{}

	excludeMaterialIDs = map[string]sets.String{
		"videobeats_operation": sets.NewString(
			"63a51a0fe99dc512b16e916b", "63bb8379b52c0f797ff1810f", "63bb83dcbdf592838b09a5d9",
			"63a52418d72a90f71ab08c19", "63bbb554b52c0f797ff18112", "63bbb4fbb52c0f797ff18111",
			"63bbb521bdf592838b09a5da", "63bb83a7b52c0f797ff18110", "63a523e9d72a90f71ab08c18",
			"638416db53b5477a729ed71f", "6384176de1da649b2cbffedf", "638417287e366f7d1575903b",
			"63745112e26f5473a81fd750", "637451938e9024f2dd0f428c", "63745166e26f5473a81fd751",
			"6250134db404242ef7d2c218", "6305ea701fbbb4d2e0a0159e", "634d3053a2a8727060a9f0a8",
			"634d30a5a2a8727060a9f0aa", "634d306ea2a8727060a9f0a9", "634d302718ff2d1020834187",
			"62aa95005ae6ac2543af58a7", "6348d692a2a8727060a9f0a0", "62f9e6fb6a3e09b6bbc52bb6",
			"626506c61ce80fa6e768f41b", "630c79f31fbbb4d2e0a015a6", "630c7a4b6a3e09b6bbc52bdd",
			"619f288b9d06e8d7a7b537c0", "619f28729d06e8d7a7b537bf", "61e7c4dc253a4e1487ec9672",
			"61e7c4ba253a4e1487ec9671", "62b27976f24cc3cfb0168e0a", "62b2795c5ae6ac2543af58bb",
			"630327e06a3e09b6bbc52bbb", "62b405ecf24cc3cfb0168e10", "62b406945ae6ac2543af58bf",
			"6306eb731fbbb4d2e0a015a1",
		),
	}
)

func init() {
	for sp, envs := range scope {
		for _, env := range envs {
			scopeEnv := fmt.Sprintf("%s_%s", sp, env)
			oldMDBName := fmt.Sprintf("%s_%s", scopeEnv, "operations_materials")
			newMDBName := fmt.Sprintf("%s_%s", scopeEnv, "operational_materials")
			oldPosDbName := fmt.Sprintf("%s_%s", scopeEnv, "material-positions-v2")
			fieldDbName := fmt.Sprintf("%s_%s", scopeEnv, "field-definitions")
			dbOldNewMap[oldMDBName] = newMDBName
			dbOldMFieldMap[oldMDBName] = fieldDbName
			dbOldPosNewMap[oldPosDbName] = newMDBName
		}
	}
}

func ClearMaterials(ctx context.Context, client *mongo.Client) error {
	for _, new := range dbOldNewMap {
		log.Printf("========= clear material %s start===========\n", new)

		newMDB := dao.NewMongodbDAO(client.Database(new), "material")

		if err := doClearMaterials(newMDB); err != nil {
			log.Printf("clear material %s  error :%v", new, err)
		}

		log.Printf("==========  clear material %s end \n", new)
	}

	return nil
}

func doClearMaterials(newM dao.MongodbDAO) error {
	t := time.UnixMilli(1669824000000)
	filter := primitive.M{"versions.updatedAt": primitive.M{"$gt": t}}
	de, err := newM.Collection().DeleteMany(context.Background(), filter)
	if err != nil {
		return err
	}

	fmt.Println(de)

	return nil
}

func SyncMaterials(ctx context.Context, client *mongo.Client) error {
	mq, cancel := InitMQ()

	for old, new := range dbOldNewMap {
		wg.Add(1)
		go func(o, n string) {
			if err := materialSync(ctx, o, n, mq, client); err != nil {
				fmt.Println(o, n, err.Error())
			}
		}(old, new)
	}

	wg.Wait()
	cancel()

	return nil
}

func materialSync(ctx context.Context, old, new string, mq event.Sender, client *mongo.Client) error {
	defer func() {
		wg.Done()
	}()

	log.Printf("========= sync material %s to %s start===========\n", old, new)
	fdb, ok := dbOldMFieldMap[old]
	if !ok {
		log.Printf("get field db name by material name %s fail \n", old)

		return fmt.Errorf("get field db name by material name")
	}
	oldMDB := dao.NewMongodbDAO(client.Database(old), "material")
	newMDB := dao.NewMongodbDAO(client.Database(new), "material")
	fieldDB := dao.NewMongodbDAO(client.Database(fdb), "fields_definition")

	sps := strings.Split(old, "_")
	if len(sps) < 2 {
		return fmt.Errorf("cant get scope and env")
	}

	scope, env := sps[0], sps[1]

	materialSyncRecoder, err := doSyncMaterial(ctx, oldMDB, newMDB, fieldDB, mq, scope, env)
	if err != nil {
		log.Printf("sync style %s to %s error :%v", old, new, err)
	}

	log.Printf("========= sync material %s to %s end=========== materialsSyncrecoder num %d\n", old, new, len(materialSyncRecoder))

	if len(materialSyncRecoder) > 0 {
		return wirteCvs(fmt.Sprintf("sync_material_%s_%s", scope, env), materialSyncRecoder)
	}

	return nil
}

func isExcludeMaterialsID(scope, env, id string) bool {
	key := scope + "_" + env
	setIDs, ok := excludeMaterialIDs[key]
	if !ok {
		return false
	}

	return setIDs.Has(id)
}

func doSyncMaterial(
	_ context.Context,
	oldm, newM, field dao.MongodbDAO,
	mq event.Sender, scope, env string,
) ([]*SyncRecoder, error) {
	syncRecoder := make([]*SyncRecoder, 0, 100)
	//test
	// ctx := context.Background()
	// id := "62d910438f4854bca96eb5dd"
	// v, err := getSingleMaterial(ctx, id, oldm)
	// if err != nil {
	// 	return err
	// }
	// var fd *api.FieldsDefinition

	// fd, err = getFieldDefine(ctx, v.TypeID, FieldCategoryMaterial, field)
	// if err != nil {
	// 	return err

	// }

	// b, _ := json.Marshal(v)
	// fmt.Println(string(b))
	// nm := v.convert(fd)
	// b, _ = json.Marshal(nm)
	// fmt.Println(string(b))
	// for i := range nm.Versions {
	// 	if err := fieldvalue.Validate(&nm.Versions[i], fd); err != nil {
	// 		return err
	// 	}
	// }
	// count, _ := newM.Collection().CountDocuments(ctx, primitive.M{"_id": nm.ID})
	// if count > 0 {
	// 	if _, err := newM.Collection().DeleteOne(ctx, primitive.M{"_id": nm.ID}); err != nil {
	// 		fmt.Println(err)
	// 	}
	// }
	// if _, err := newM.Collection().UpdateOne(
	// 	ctx, primitive.M{"_id": nm.ID}, op.Set(nm),
	// 	options.Update().SetUpsert(true),
	// ); err != nil {
	// 	return err
	// }

	// if err := sendMaterialCreateMessage(ctx, mq, scope, env, []*Material{nm}); err != nil {
	// 	fmt.Printf("send material msg fail, err: %s", err.Error())
	// }

	ctx := context.Background()
	page := int32(1)
	hasNext := true
	for hasNext {
		materilCreats := make([]*Material, 0)
		res, hn, err := getSyncDatats[OldMaterial](ctx, oldm, page)
		if err != nil {
			return nil, err
		}

		fdCache := make(map[string]*api.FieldsDefinition)
		for _, v := range res {
			// 排除一些产品空间中的素材不同步
			if !isExcludeMaterialsID(scope, env, v.ID.Hex()) {
				continue
			}

			var fd *api.FieldsDefinition
			key := fmt.Sprintf("%s_%d", v.TypeID, FieldCategoryMaterial)
			if fdc, ok := fdCache[key]; ok {
				fd = fdc
			} else {
				fd, err = getFieldDefine(ctx, v.TypeID, FieldCategoryMaterial, field)
				if err != nil {
					log.Printf("get field define by %s error: %s", v.TypeID, err)
					syncRecoder = append(syncRecoder, NewSyncRecoder(
						oldm.Collection().Name(),
						v.ID.Hex(),
						err.Error(),
						scope,
						env,
						0,
					))

					continue
				}

				fdCache[key] = fd
			}

			nm := v.convert(fd)
			// 同步数据不校验 即使数据不正确也应该同步 用户下一次编辑的时候会提示错误修正即可
			// var verr error
			// for i := range nm.Versions {
			// 	if verr = fieldvalue.Validate(&nm.Versions[i], fd); verr != nil {
			// 		materialSyncErr = append(materialSyncErr, NewSyncRecoder(
			// 			oldm.Collection().Name(),
			// 			v.ID.Hex(),
			// 			verr.Error(),
			// 			scope,
			// 			env,
			// 			0,
			// 		))

			// 		break
			// 	}
			// }
			// if verr != nil {
			// 	continue
			// }

			count, _ := newM.Collection().CountDocuments(ctx, primitive.M{"_id": nm.ID})
			if count > 0 {
				if _, err := newM.Collection().DeleteOne(ctx, primitive.M{"_id": nm.ID}); err != nil {
					fmt.Println(err)
				}
			}

			if _, err := newM.Collection().UpdateOne(
				ctx, primitive.M{"_id": nm.ID}, op.Set(nm),
				options.Update().SetUpsert(true),
			); err != nil {
				log.Printf("insert %s error: %s", v.TypeID, err)
				syncRecoder = append(syncRecoder, NewSyncRecoder(
					oldm.Collection().Name(),
					v.ID.Hex(),
					err.Error(),
					scope,
					env,
					0,
				))

				continue
			}
			materilCreats = append(materilCreats, nm)
		}
		if len(materilCreats) > 0 {
			if err := sendMaterialCreateMessage(ctx, mq, scope, env, materilCreats); err != nil {
				fmt.Printf("send material msg fail, err: %s", err.Error())
			}
		}
		page++
		hasNext = hn
	}

	return syncRecoder, nil
}

func SyncMaterialCategorys(ctx context.Context, client *mongo.Client) error {
	mq, cancel := InitMQ()
	defer cancel()
	for old, new := range dbOldNewMap {
		log.Printf("========= sync material categorys %s to %s start===========\n", old, new)
		fdb, ok := dbOldMFieldMap[old]
		if !ok {
			log.Printf("get field db name by material category name %s fail \n", old)

			return fmt.Errorf("get field db name by material  category name")
		}
		oldMDB := dao.NewMongodbDAO(client.Database(old), "material_category")
		newMDB := dao.NewMongodbDAO(client.Database(new), "material_category")
		fieldDB := dao.NewMongodbDAO(client.Database(fdb), "fields_definition")

		sps := strings.Split(old, "_")
		if len(sps) < 2 {
			return fmt.Errorf("cant get scope and env")
		}

		scope, env := sps[0], sps[1]

		if err := doSyncMaterialCategory(ctx, oldMDB, newMDB, fieldDB, mq, scope, env); err != nil {
			log.Printf("sync category  %s to %s error :%v", old, new, err)
		}

		log.Printf("========== run sync %s to %s end \n", old, new)
	}

	if len(categorySyncErr) > 0 {
		return wirteCvs("sync_material_category", categorySyncErr)
	}

	return nil
}

func doSyncMaterialCategory(
	_ context.Context, oldm, newM, field dao.MongodbDAO, mq event.Sender, scope, env string,
) error {
	ctx := context.Background()
	page := int32(1)
	hasNext := true
	for hasNext {
		creats := make([]*Category, 0)
		res, hn, err := getSyncDatats[OldCategory](ctx, oldm, page)
		if err != nil {
			return err
		}

		fdCache := make(map[string]*api.FieldsDefinition)
		for _, v := range res {
			var fd *api.FieldsDefinition
			key := fmt.Sprintf("%s_%d", v.TypeID, FieldCategoryMaterialCate)
			if fdc, ok := fdCache[key]; ok {
				fd = fdc
			} else {
				fd, err = getFieldDefine(ctx, v.TypeID, FieldCategoryMaterialCate, field)
				if err != nil {
					log.Printf("get field define by %s error: %s", v.TypeID, err)
					categorySyncErr = append(categorySyncErr, NewSyncRecoder(
						oldm.Collection().Name(),
						v.ID.Hex(),
						err.Error(),
						scope,
						env,
						0,
					))

					continue
				}

				fdCache[key] = fd
			}
			// b, _ := json.Marshal(v)
			// fmt.Println(string(b))
			nm := v.convert(fd)
			// b, _ = json.Marshal(nm)
			// fmt.Println(string(b))
			count, _ := newM.Collection().CountDocuments(ctx, primitive.M{"_id": nm.ID})
			if count > 0 {
				if _, err := newM.Collection().DeleteOne(ctx, primitive.M{"_id": nm.ID}); err != nil {
					fmt.Println(err)
				}
			}
			if _, err := newM.Collection().UpdateOne(
				ctx, primitive.M{"_id": nm.ID}, op.Set(nm),
				options.Update().SetUpsert(true),
			); err != nil {
				log.Printf("insert %s error: %s", v.TypeID, err)
				categorySyncErr = append(categorySyncErr, NewSyncRecoder(
					oldm.Collection().Name(),
					v.ID.Hex(),
					err.Error(),
					scope,
					env,
					0,
				))

				continue
			}
			creats = append(creats, nm)
		}
		if len(creats) > 0 {
			if err := sendCategoryCreateMessage(ctx, mq, scope, env, creats); err != nil {
				fmt.Printf("send material msg fail, err: %s", err.Error())
			}
		}
		page++
		hasNext = hn
	}

	return nil
}

func getSyncDatats[T OldCategory | OldMaterial | MaterialPosition | Plan | Material](
	ctx context.Context, mdb dao.MongodbDAO, page int32,
) ([]*T, bool, error) {
	opts := &FindOptions{}
	opts.Pagination().SetPage(page).SetPageSize(10)
	res := []*T{}
	total := int32(0)
	err := mdb.Find(ctx, &res, opts)
	if err != nil {
		return nil, false, err
	}

	if opts.Pagination() != nil {
		total = opts.Pagination().TotalPage
	}

	return res, total > page, err
}

func getSingleMaterial(ctx context.Context, id string, mdb dao.MongodbDAO) (*OldMaterial, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}

	doc := new(OldMaterial)

	err = mdb.Collection().FindOne(ctx, primitive.M{"_id": oid}).Decode(doc)

	return doc, err
}

func GetSingleNewMaterial(ctx context.Context, id string, mdb dao.MongodbDAO) (*Material, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}

	doc := new(Material)

	err = mdb.Collection().FindOne(ctx, primitive.M{"_id": oid}).Decode(doc)

	return doc, err
}

func getFieldDefine(ctx context.Context, fid string, tp FieldCategory, mdb dao.MongodbDAO) (*api.FieldsDefinition, error) {
	doc := new(FieldsDefinition)
	err := mdb.Collection().FindOne(ctx, primitive.M{"_id": fid, "type": tp}).Decode(doc)
	if err != nil {
		return nil, err
	}

	return apiFieldsDefinition(doc), nil
}

func wirteCvs(app string, recoder []*SyncRecoder) error {
	csvFile := fmt.Sprintf("./%s_%d.csv", app, time.Now().Unix())
	fh, err := os.OpenFile(csvFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	fw := csv.NewWriter(fh)
	rows := make([][]string, 0, len(recoder))
	for _, v := range recoder {
		rowCSV := make([]string, 0, 5)
		rowCSV = append(rowCSV, v.DBFullColl)
		rowCSV = append(rowCSV, v.ID)
		if v.Type == 0 {
			rowCSV = append(rowCSV, "material")
		} else {
			rowCSV = append(rowCSV, "category")
		}
		rowCSV = append(rowCSV, v.Err)
		rowCSV = append(rowCSV, v.Scope)
		rowCSV = append(rowCSV, v.Env)
		rows = append(rows, rowCSV)
	}

	return fw.WriteAll(rows)
}
