package material

import (
	"context"
	"errors"
	"fmt"

	"github.com/pinguo-icc/go-lib/v2/dao"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CreateMapBetweenBmallAndOpsID 建立从 bmall 迁移的素材的id的映射关系
func CreateMapBetweenBmallAndOpsID(ctx context.Context, ops, bmall *mongo.Client) error {
	opsDBName := "camera360_prod_operational_materials"
	bmallDBName := "bmall"
	opsDao := dao.NewMongodbDAO(ops.Database(opsDBName), "material")
	bmallDao := dao.NewMongodbDAO(bmall.Database(bmallDBName), "id_relationship")

	// 在ops中分叶查找有bmallID 或者有 bmallIDArd 的素材
	fOpt := &FindBmallOptions{}
	hasNext := true
	page := 1

	for hasNext {
		fmt.Println("process ==> ", page)
		fOpt.Pagination().SetPage(int32(page)).SetPageSize(20)
		res := make([]*Material, 0, 20)
		err := opsDao.Find(ctx, &res, fOpt)
		if err != nil {
			fmt.Println("load materials data occur error: ", err.Error(), "page: ", page, "pageSize: ", 20)

			return err
		}
		total := int32(0)
		if fOpt.Pagination() != nil {
			total = fOpt.Pagination().TotalPage
		}

		hasNext = total > int32(page)
		page++

		syncMaterialsID(res, bmallDao)
	}

	fmt.Println("total:", gTotal, "repeated:", gRepeated, "materialsCount: ", mCount, "notOk:", gNotOk)

	return nil
}

func syncMaterialsID(materials []*Material, bmallDao dao.MongodbDAO) {
	for _, m := range materials {
		mCount++
		if m.IsDeleted {
			continue
		}
		res := GenRelationshipID(m)
		if len(res) > 0 {
			for _, r := range res {
				saveRelationshipID(r, bmallDao)
			}
		}
	}
}

var gTotal = 0
var gRepeated = 0
var gNotOk = 0
var mCount = 0

func saveRelationshipID(r *RelationshipID, bmallDao dao.MongodbDAO) {
	ctx := context.Background()
	or := &RelationshipID{}
	err := bmallDao.Collection().FindOne(ctx, primitive.M{"_id": r.BmallID}).Decode(or)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			fmt.Println("saveRelationshipID: ", err.Error(), "id: ", r.BmallID.Hex())

			return
		}
		// 执行插入操作
		if _, err := bmallDao.Collection().InsertOne(ctx, r); err != nil {
			fmt.Println("saveRelationshipID.insert : ", err.Error(), "id: ", r.BmallID.Hex())

			return
		}

		return
	}

	// 执行更新操作
	if r.BmallID == or.BmallID && r.OpsID == or.OpsID {
		gRepeated++
		return
	}

	fmt.Println("need update: new-> ", r.BmallID.Hex(), r.OpsID, "old-> ", or.BmallID.Hex(), or.OpsID)

	_, err = bmallDao.Collection().UpdateByID(ctx, r.BmallID, primitive.M{"$set": primitive.M{"opsID": r.OpsID}}, options.Update().SetUpsert(true))
	if err != nil {
		fmt.Println("saveRelationshipID.update : ", err.Error(), "id: ", r.BmallID.Hex())
	}
}

type FindBmallOptions struct {
	dao.MongodbFindOptions
}

func (opt *FindBmallOptions) Filter() (interface{}, error) {
	return primitive.M{
		"isDeleted": false,
		"$or": primitive.A{
			primitive.M{
				"versions.custom.bmallId": primitive.M{"$exists": 1},
			},
			primitive.M{
				"versions.custom.bmallIdArd": primitive.M{"$exists": 1},
			},
		},
	}, nil
}

func (opt *FindBmallOptions) Sorts() dao.SortFields {
	// 去除lint警告而已,后续通过lint检测配置处理
	_ = opt

	return dao.SortFields{
		{
			Name:      "_id",
			Direction: dao.SortFieldDesc,
		},
	}
}

type RelationshipID struct {
	BmallID primitive.ObjectID `bson:"_id"`
	OpsID   string             `bson:"opsID"`
}

func GenRelationshipID(m *Material) []*RelationshipID {
	if m == nil || len(m.Versions) == 0 {
		fmt.Errorf("can't get material id relationship id, maybe material data error")
		return nil
	}
	opsID := m.ID.Hex()
	res := []*RelationshipID{}
	if bmallId, ok := m.Versions[0].Custom["bmallId"]; ok {
		if bid := bmallId.GetText(); bid != "" {
			oid, err := primitive.ObjectIDFromHex(bid)
			if err != nil {
				fmt.Println("material: "+opsID+" bmallId: "+bid+" is invalidate.", err.Error())
			} else {
				res = append(res, &RelationshipID{BmallID: oid, OpsID: opsID})
			}
			gTotal++
		} else {
			gTotal++
			gNotOk++
		}
	} else {
		gTotal++
		gNotOk++
	}

	if bmallArdId, ok := m.Versions[0].Custom["bmallIdArd"]; ok {
		if bid := bmallArdId.GetText(); bid != "" {
			oid, err := primitive.ObjectIDFromHex(bid)
			if err != nil {
				fmt.Println("material: "+opsID+" bmallId: "+bid+" is invalidate.", err.Error())
			} else {
				res = append(res, &RelationshipID{BmallID: oid, OpsID: opsID})
			}
			gTotal++
		} else {
			gTotal++
			gNotOk++
		}
	} else {
		gTotal++
		gNotOk++
	}

	return res
}
