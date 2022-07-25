package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

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

func (e *dbEntity) run(ctx context.Context, client *mongo.Client) {
	for _, v := range e.dbNames() {
		log.Printf("======run on db %s ===========\n", v)

		db := client.Database(v)
		for _, c := range e.coll {
			col := db.Collection(c)
			if err := setSoftDeleteField(ctx, col); err != nil {
				log.Println(err)
			}
		}

		log.Printf("======run finished =========== \n\n")
	}
}

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

var execs []dbEntity

func init() {
	execs = []dbEntity{
		{
			dbName: "material-positions-v2",
			coll:   []string{"materialPosition", "plan"},
			scope: map[string][]string{
				"videobeats": {"prod", "operation", "dev", "qa"},
				"camera360":  {"prod", "operation", "dev", "qa"},
				"idphoto":    {"prod", "operation", "dev", "qa"},
				"mix":        {"prod", "operation", "dev", "qa"},
				"icc":        {"prod", "operation", "dev", "qa"},
			},
		},
		{
			dbName: "operational-positions",
			coll:   []string{"activity", "activity_plan", "position"},
			scope: map[string][]string{
				"camera360":  {"prod", "operation", "dev", "qa"},
				"videobeats": {"prod", "operation", "dev", "qa"},
				"idphoto":    {"prod", "operation", "dev", "qa"},
				"mix":        {"prod", "operation", "dev", "qa"},
				"icc":        {"prod", "operation", "dev", "qa"},
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
		v.run(ctx, cli)
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

	exec(ctx, dbCli)
}
