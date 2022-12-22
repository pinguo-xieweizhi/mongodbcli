package material

import (
	"context"
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
				fmt.Println(v.ID.Hex(), err.Error())

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
				fmt.Println(v.ID.Hex(), err.Error())

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
