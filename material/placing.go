package material

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/pinguo-icc/go-base/v2/event"
	"github.com/pinguo-icc/go-lib/v2/dao"
	"github.com/pinguo-icc/kratos-library/mongo/op"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func SyncMaterialsPosition(ctx context.Context, client *mongo.Client) error {
	mq, cancel := InitMQ()
	defer cancel()
	defer func() {
		if len(materialPositionSyncErr) > 0 {
			_ = wirteCvs("sync_material_position", materialPositionSyncErr)
		}
	}()
	for old, new := range dbOldPosNewMap {
		log.Printf("========= sync material_position %s to %s start===========\n", old, new)
		oldMDB := dao.NewMongodbDAO(client.Database(old), "materialPosition")
		newMDB := dao.NewMongodbDAO(client.Database(new), "materialPosition")

		sps := strings.Split(old, "_")
		if len(sps) < 2 {
			return fmt.Errorf("cant get scope and env")
		}

		scope, env := sps[0], sps[1]

		if err := doSyncMaterialPosition(ctx, oldMDB, newMDB, mq, scope, env); err != nil {
			log.Printf("sync style %s to %s error :%v", old, new, err)
		}

		log.Printf("========== run sync %s to %s end \n", old, new)
	}

	return nil
}

func doSyncMaterialPosition(_ context.Context, oldMDB, newMDB dao.MongodbDAO, mq event.Sender, scope, env string) error {
	ctx := context.Background()
	page := int32(1)
	hasNext := true
	for hasNext {
		res, hn, err := getSyncDatats[MaterialPosition](ctx, oldMDB, page)
		if err != nil {
			return err
		}

		mes := make([]*MaterialPosition, 0, len(res))
		for _, v := range res {
			if _, err := newMDB.Collection().UpdateOne(
				ctx, primitive.M{"_id": v.ID}, op.Set(v),
				options.Update().SetUpsert(true),
			); err != nil {
				materialPositionSyncErr = append(materialPositionSyncErr, NewSyncRecoder(
					oldMDB.Collection().Name(),
					v.ID.Hex(),
					err.Error(),
					scope,
					env,
					int(page),
				))

				continue
			}

			mes = append(mes, v)
		}

		if len(mes) > 0 {
			if err := sendMaterialPositionCreateMessage(ctx, mq, scope, env, res); err != nil {
				fmt.Printf("send material msg fail, err: %s", err.Error())
			}
		}

		page++
		hasNext = hn
	}

	return nil
}

func SyncMaterialsPlan(ctx context.Context, client *mongo.Client) error {
	mq, cancel := InitMQ()
	defer cancel()
	defer func() {
		if len(planPositionSyncErr) > 0 {
			_ = wirteCvs("sync_material_plan", planPositionSyncErr)
		}
	}()
	for old, new := range dbOldPosNewMap {
		log.Printf("========= sync material_position %s to %s start===========\n", old, new)
		oldMDB := dao.NewMongodbDAO(client.Database(old), "plan")
		newMDB := dao.NewMongodbDAO(client.Database(new), "plan")

		sps := strings.Split(old, "_")
		if len(sps) < 2 {
			return fmt.Errorf("cant get scope and env")
		}

		scope, env := sps[0], sps[1]

		if err := doSyncMaterialPlan(ctx, oldMDB, newMDB, mq, scope, env); err != nil {
			log.Printf("sync style %s to %s error :%v", old, new, err)
		}

		log.Printf("========== run sync %s to %s end \n", old, new)
	}

	return nil
}

func doSyncMaterialPlan(_ context.Context, oldMDB, newMDB dao.MongodbDAO, mq event.Sender, scope, env string) error {
	ctx := context.Background()
	page := int32(1)
	hasNext := true
	for hasNext {
		res, hn, err := getSyncDatats[Plan](ctx, oldMDB, page)
		if err != nil {
			return err
		}

		mes := make([]*Plan, 0, len(res))
		for _, v := range res {
			if _, err := newMDB.Collection().UpdateOne(
				ctx, primitive.M{"_id": v.ID}, op.Set(v),
				options.Update().SetUpsert(true),
			); err != nil {
				planPositionSyncErr = append(planPositionSyncErr, NewSyncRecoder(
					oldMDB.Collection().Name(),
					v.ID.Hex(),
					err.Error(),
					scope,
					env,
					int(page),
				))

				continue
			}

			mes = append(mes, v)
		}

		if len(mes) > 0 {
			if err := sendMaterialPlanCreateMessage(ctx, mq, scope, env, res); err != nil {
				fmt.Printf("send material msg fail, err: %s", err.Error())
			}
		}
		page++
		hasNext = hn
	}

	return nil
}

func DealPlan(_ context.Context, client *mongo.Client) error {
	inner := func(db dao.MongodbDAO, scope, env string) {
		ctx := context.Background()
		cond := primitive.D{
			{"$or",
				primitive.A{
					primitive.D{
						{"$nor",
							primitive.A{
								primitive.D{{"placingContents.categories.materials", primitive.D{{"$exists", false}}}},
								primitive.D{{"placingContents.categories.materials", primitive.D{{"$size", 0}}}},
							},
						},
						{"$or",
							primitive.A{
								primitive.D{
									{"placingContents.categories.materials.vip", primitive.D{{"$exists", true}}},
									{"placingContents.categories.materials.vip", primitive.D{{"$ne", primitive.Null{}}}},
								},
								primitive.D{
									{"placingContents.categories.materials.period", primitive.D{{"$exists", true}}},
									{"placingContents.categories.materials.period", primitive.D{{"$ne", primitive.Null{}}}},
								},
							},
						},
					},
					primitive.D{
						{"$nor",
							primitive.A{
								primitive.D{{"placingContents.materials", primitive.D{{"$exists", false}}}},
								primitive.D{{"placingContents.materials", primitive.D{{"$size", 0}}}},
							},
						},
						{"$or",
							primitive.A{
								primitive.D{
									{"placingContents.materials.vip", primitive.D{{"$exists", true}}},
									{"placingContents.materials.vip", primitive.D{{"$ne", primitive.Null{}}}},
								},
								primitive.D{
									{"placingContents.materials.period", primitive.D{{"$exists", true}}},
									{"placingContents.materials.period", primitive.D{{"$ne", primitive.Null{}}}},
								},
							},
						},
					},
				},
			},
		}

		res, err := db.Collection().CountDocuments(ctx, cond)
		if err != nil {
			fmt.Println(err)
		}

		if res > 0 {
			msg := fmt.Sprintf("%s 有%d条投放覆盖数据%s-%s", db.Collection().Name(), res, scope, env)
			fmt.Println(msg)
		}
	}

	for old, new := range dbOldPosNewMap {
		log.Printf("========= count material plan overrade %s ===========\n", new)
		newMDB := dao.NewMongodbDAO(client.Database(new), "plan")

		sps := strings.Split(old, "_")
		if len(sps) < 2 {
			return fmt.Errorf("cant get scope and env")
		}

		scope, env := sps[0], sps[1]

		inner(newMDB, scope, env)

		log.Printf("========== count material plan overrade %s  \n", new)
	}

	return nil
}

// 检查是否有覆盖数据
func DealWithPlanBytraverse(_ context.Context, client *mongo.Client) error {
	inner := func(db, mdb dao.MongodbDAO, scope, env string) error {
		page := int32(1)
		hasNext := true
		count := 0
		ids := make([]string, 0)
		for hasNext {
			res, hn, err := getSyncDatats[Plan](context.Background(), db, page)
			if err != nil {
				return err
			}

			for _, v := range res {
				hasOverride, err := v.newOverrideMaterialsVersion(mdb)
				if err != nil {
					return err
				}
				if hasOverride {
					// TODO: 需更新计划
					count++
					ids = append(ids, v.ID.Hex())
					bt, err := json.Marshal(v)
					if err == nil {
						fmt.Println(string(bt))
					}
				}
			}

			bt, err := json.Marshal(cacheOverrideVersionID)
			if err == nil {
				fmt.Println(string(bt))
			}

			page++
			hasNext = hn
		}

		if count > 0 {
			msg := fmt.Sprintf("%s 有%d条投放覆盖数据%s-%s: ids:%s", db.Collection().Name(), count, scope, env, strings.Join(ids, ","))
			fmt.Println(msg)
		}

		return nil
	}
	for old, new := range dbOldPosNewMap {
		log.Printf("========= count material plan overrade %s ===========\n", new)
		newMDB := dao.NewMongodbDAO(client.Database(new), "plan")
		newDBMaterils := dao.NewMongodbDAO(client.Database(new), "material")
		sps := strings.Split(old, "_")
		if len(sps) < 2 {
			return fmt.Errorf("cant get scope and env")
		}

		scope, env := sps[0], sps[1]

		if err := inner(newMDB, newDBMaterils, scope, env); err != nil {
			fmt.Println(err)
		}

		log.Printf("========== count material plan overrade %s  \n", new)
	}

	return nil
}
