package material

import (
	"context"
	"fmt"
	"time"

	"github.com/pinguo-icc/go-base/v2/event"
	mapi "github.com/pinguo-icc/operational-materials-svc/api"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	kafkaAddrs = []string{
		"a4c75dcbd52214b55bd344496bdb5901-f4ea52a09ad4dc1d.elb.cn-northwest-1.amazonaws.com.cn:32000",
		"ad435efd7df774eceac057d854557280-e47fe7160bccdf15.elb.cn-northwest-1.amazonaws.com.cn:32000",
		"a39796aa1f0324c6d9ca4d32e4ced8dd-67c2f7f060c9452a.elb.cn-northwest-1.amazonaws.com.cn:32000",
	}
)

func InitMQ() (event.Sender, func()) {
	return event.NewKafkaSender(&event.Config{
		Base: event.BaseConfig{},
		Write: event.WriteConfig{
			Addr: kafkaAddrs,
		},
		Read: event.ReaderConfig{},
	})
}

func sendMaterialCreateMessage(ctx context.Context, mq event.Sender, scope, env string, datas []*Material) error {
	topic := fmt.Sprintf("%s.%s.operate", "operational-materials-svc", "material")
	headers := map[string]string{
		"scope": scope,
		"env":   env,
	}
	msg := []event.Event{}
	for _, v := range datas {
		traceID := primitive.NewObjectID().Hex()
		fmt.Println("traceID", traceID)
		oe := &mapi.MaterialOperateEvent{
			Data:        []*mapi.Material{convertMaterialToAPI(ctx, v)},
			OperateType: "Create",
			OperatedAt:  time.Now().Unix(),
		}

		data, err := protojson.Marshal(oe)
		if err != nil {
			return err
		}

		msg = append(msg, event.NewEvent(
			data,
			v.ID.Hex(),
			event.WithHeaders(headers),
			event.WithTopic(topic),
			event.WithHeaderTrackID(traceID),
		))
	}

	return mq.Send(ctx, msg...)
}

func sendCategoryCreateMessage(ctx context.Context, mq event.Sender, scope, env string, datas []*Category) error {
	topic := fmt.Sprintf("%s.%s.operate", "operational-materials-svc", "material_category")
	headers := map[string]string{
		"scope": scope,
		"env":   env,
	}
	msg := []event.Event{}
	for _, v := range datas {
		traceID := primitive.NewObjectID().Hex()
		fmt.Println("traceID", traceID)
		oe := &mapi.CategoryOperateEvent{
			Data:        []*mapi.Category{convertCategoryToAPI(ctx, v, false)},
			OperateType: "Create",
			OperatedAt:  time.Now().Unix(),
		}
		data, err := protojson.Marshal(oe)
		if err != nil {
			return err
		}

		msg = append(msg, event.NewEvent(
			data,
			v.ID.Hex(),
			event.WithHeaders(headers),
			event.WithTopic(topic),
			event.WithHeaderTrackID(traceID),
		))
	}

	return mq.Send(ctx, msg...)
}

func sendMaterialPositionCreateMessage(ctx context.Context, mq event.Sender, scope, env string, datas []*MaterialPosition) error {
	topic := fmt.Sprintf("%s.%s.operate", "operational-materials-svc", "materialPosition")
	headers := map[string]string{
		"scope": scope,
		"env":   env,
	}
	msg := []event.Event{}
	for _, v := range datas {
		traceID := primitive.NewObjectID().Hex()
		fmt.Println("traceID", traceID)
		oe := &mapi.MaterialPositionOperateEvent{
			Data:        []*mapi.MaterialPosition{materialPositionToAPI(v)},
			OperateType: "Create",
			OperatedAt:  time.Now().Unix(),
		}

		data, err := protojson.Marshal(oe)
		if err != nil {
			return err
		}

		msg = append(msg, event.NewEvent(
			data,
			v.ID.Hex(),
			event.WithHeaders(headers),
			event.WithTopic(topic),
			event.WithHeaderTrackID(traceID),
		))
	}

	return mq.Send(ctx, msg...)
}

func sendMaterialPlanCreateMessage(ctx context.Context, mq event.Sender, scope, env string, datas []*Plan) error {
	topic := fmt.Sprintf("%s.%s.operate", "operational-materials-svc", "plan")
	headers := map[string]string{
		"scope": scope,
		"env":   env,
	}
	msg := []event.Event{}
	for _, v := range datas {
		traceID := primitive.NewObjectID().Hex()
		fmt.Println("traceID", traceID)
		oe := &mapi.MaterialPlanOperateEvent{
			Data:        []*mapi.Plan{cplan.APIPlan(v)},
			OperateType: "Create",
			OperatedAt:  time.Now().Unix(),
		}

		data, err := protojson.Marshal(oe)
		if err != nil {
			return err
		}

		msg = append(msg, event.NewEvent(
			data,
			v.ID.Hex(),
			event.WithHeaders(headers),
			event.WithTopic(topic),
			event.WithHeaderTrackID(traceID),
		))
	}

	return mq.Send(ctx, msg...)
}
