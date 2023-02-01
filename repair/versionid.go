package repair

import (
	"context"
	"fmt"
	"sync"

	"github.com/pinguo-icc/go-lib/v2/dao"
	"github.com/pinguo-icc/mongodbcli/material"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"k8s.io/apimachinery/pkg/util/sets"
)

var (
	scope = map[string][]string{
		//"videobeats": {"prod", "dev", "qa", "pre"},
		//"camera360":  {"prod", "dev", "qa", "pre"},
		//"idphoto":    {"prod", "dev", "qa", "pre"},
		//"mix":        {"prod", "dev", "qa", "pre"},
		//"salad":      {"prod", "dev", "qa", "pre"},
		//"inface":     {"prod", "dev", "qa", "pre"},
		"icc": {"prod", "dev", "qa", "pre"},
		//"april":      {"prod", "dev", "qa", "pre"},
		// "videobeats": {"operation"},
	}

	//scope = map[string][]string{
	//	"videobeats": {"prod", "operation", "dev", "qa", "pre"},
	//	"camera360":  {"prod", "operation", "dev", "qa", "pre"},
	//	"idphoto":    {"prod", "operation", "dev", "qa", "pre"},
	//	"mix":        {"prod", "operation", "dev", "qa", "pre"},
	//	"salad":      {"prod", "operation", "dev", "qa", "pre"},
	//	"inface":     {"prod", "operation", "dev", "qa", "pre"},
	//	"icc":        {"prod", "operation", "dev", "qa", "pre"},
	//	"april":      {"prod", "operation", "dev", "qa", "pre"},
	//	// "videobeats": {"operation"},
	//}

	wg = sync.WaitGroup{}

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

func Index(ctx context.Context, client *mongo.Client) error {
	dbs := make(map[string]dao.MongodbDAO)
	for sp, envs := range scope {
		for _, env := range envs {
			scopeEnv := fmt.Sprintf("%s_%s", sp, env)
			spaceName := fmt.Sprintf("%s_%s", scopeEnv, "operational_materials")
			dbs[spaceName] = dao.NewMongodbDAO(client.Database(spaceName), "material")
		}
	}

	for sp, envs := range scope {
		operationDBName := fmt.Sprintf("%s_operation_%s", sp, "operational_materials")
		operationDB := dao.NewMongodbDAO(client.Database(operationDBName), "material")
		cur, err := operationDB.Collection().Find(ctx, primitive.M{})
		if err != nil {
			panic(err)
		}

		for cur.Next(ctx) {
			ret := &material.Material{}
			if err := cur.Decode(&ret); err != nil {
				panic(err)
			}

			versionID := ret.Versions[0].VersionID

			for _, env := range envs {
				scopeEnv := fmt.Sprintf("%s_%s", sp, env)
				spaceName := fmt.Sprintf("%s_%s", scopeEnv, "operational_materials")
				otherSpace, err := material.GetSingleNewMaterial(context.Background(), ret.ID.Hex(), dbs[spaceName])
				if err != nil {
					if err == mongo.ErrNoDocuments {
						continue
					}

					fmt.Println("material.GetSingleNewMaterial id", ret.ID.Hex(), " err ", err.Error())
				}

				otherSpace.Versions[0].VersionID = versionID
				updateData := primitive.M{"$set": primitive.M{"versions": otherSpace.Versions}}
				_, updateErr := dbs[spaceName].Collection().UpdateOne(context.Background(), primitive.M{"_id": otherSpace.ID}, updateData)
				if updateErr != nil {
					fmt.Println("UpdateOne spaceName:", spaceName, " id ", otherSpace.ID.Hex())
				}
			}

		}

	}

	return nil
}
