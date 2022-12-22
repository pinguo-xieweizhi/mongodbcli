package material

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pinguo-icc/field-definitions/api"
	"github.com/pinguo-icc/go-base/v2/event"
	"github.com/pinguo-icc/go-lib/v2/dao"
	"github.com/pinguo-icc/kratos-library/mongo/op"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
		// "camera360": {"dev"},
	}
	dbOldNewMap     = make(map[string]string, 0)
	dbOldMFieldMap  = make(map[string]string, 0)
	dbOldPosNewMap  = make(map[string]string, 0)
	materialSyncErr = make([]*SyncRecoder, 0, 100)
	categorySyncErr = make([]*SyncRecoder, 0, 100)
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

func SyncMaterials(ctx context.Context, client *mongo.Client) error {
	mq, cancel := InitMQ()
	defer cancel()
	for old, new := range dbOldNewMap {
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

		if err := doSyncMaterial(ctx, oldMDB, newMDB, fieldDB, mq, scope, env); err != nil {
			log.Printf("sync style %s to %s error :%v", old, new, err)
		}

		log.Printf("========== run sync %s to %s end \n", old, new)
	}

	if len(materialSyncErr) > 0 {
		return wirteCvs("sync_material", materialSyncErr)
	}

	return nil
}

func doSyncMaterial(_ context.Context, oldm, newM, field dao.MongodbDAO, mq event.Sender, scope, env string) error {
	//test
	// ctx := context.Background()
	// id := "632d5fded3806c045b9e3bde"
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

	// count, _ := newM.Collection().CountDocuments(ctx, primitive.M{"_id": nm.ID})
	// if count > 0 {
	// 	if _, err := newM.Collection().DeleteOne(ctx, primitive.M{"_id": nm.ID}); err != nil {
	// 		fmt.Println(err)
	// 	}
	// }

	// if _, err := newM.Collection().DeleteOne(ctx, primitive.M{"_id": nm.ID}); err != nil {
	// 	fmt.Println(err)
	// }
	// if _, err := newM.Collection().UpdateOne(
	// 	ctx, primitive.M{"_id": nm.ID}, op.Set(nm),
	// 	options.Update().SetUpsert(true),
	// ); err != nil {
	// 	return err
	// }

	ctx := context.Background()
	page := int32(1)
	hasNext := true
	for hasNext {
		materilCreats := make([]*Material, 0)
		res, hn, err := getSyncDatats[OldMaterial](ctx, oldm, page)
		if err != nil {
			return err
		}

		fdCache := make(map[string]*api.FieldsDefinition)
		for _, v := range res {
			var fd *api.FieldsDefinition
			key := fmt.Sprintf("%s_%d", v.TypeID, FieldCategoryMaterial)
			if fdc, ok := fdCache[key]; ok {
				fd = fdc
			} else {
				fd, err = getFieldDefine(ctx, v.TypeID, FieldCategoryMaterial, field)
				if err != nil {
					log.Printf("get field define by %s error: %s", v.TypeID, err)
					materialSyncErr = append(materialSyncErr, NewSyncRecoder(
						oldm.Collection().Name(),
						v.ID.Hex(),
						err.Error(),
						0,
					))

					continue
				}

				fdCache[key] = fd
			}

			nm := v.convert(fd)
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
				materialSyncErr = append(materialSyncErr, NewSyncRecoder(
					oldm.Collection().Name(),
					v.ID.Hex(),
					err.Error(),
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

	return nil
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
					materialSyncErr = append(materialSyncErr, NewSyncRecoder(
						oldm.Collection().Name(),
						v.ID.Hex(),
						err.Error(),
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
				materialSyncErr = append(materialSyncErr, NewSyncRecoder(
					oldm.Collection().Name(),
					v.ID.Hex(),
					err.Error(),
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

func getSyncDatats[T OldCategory | OldMaterial | MaterialPosition | Plan](
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
		rows = append(rows, rowCSV)
	}

	return fw.WriteAll(rows)
}
