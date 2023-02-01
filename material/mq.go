package material

import (
	"context"
	"fmt"
	"time"

	"github.com/pinguo-icc/go-base/v2/event"
	h5api "github.com/pinguo-icc/operational-h5-svc/api"
	mapi "github.com/pinguo-icc/operational-materials-svc/api"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	// dev
	// kafkaAddrs = []string{
	// 	"a4c75dcbd52214b55bd344496bdb5901-f4ea52a09ad4dc1d.elb.cn-northwest-1.amazonaws.com.cn:32000",
	// 	"ad435efd7df774eceac057d854557280-e47fe7160bccdf15.elb.cn-northwest-1.amazonaws.com.cn:32000",
	// 	"a39796aa1f0324c6d9ca4d32e4ced8dd-67c2f7f060c9452a.elb.cn-northwest-1.amazonaws.com.cn:32000",
	// }

	// QA
	// kafkaAddrs = []string{
	// 	"47.97.215.66:32000",
	// 	"118.31.75.196:32000",
	// 	"118.31.42.178:32000",
	// }

	// prod
	kafkaAddrs = []string{
		"alikafka-pre-cn-uqm32h3zm00b-1-vpc.alikafka.aliyuncs.com:9092",
		"alikafka-pre-cn-uqm32h3zm00b-2-vpc.alikafka.aliyuncs.com:9092",
		"alikafka-pre-cn-uqm32h3zm00b-3-vpc.alikafka.aliyuncs.com:9092",
	}
)

func InitMQ() (event.Sender, func()) {
	return event.NewKafkaSender(&event.Config{
		Base: event.BaseConfig{},
		Write: event.WriteConfig{
			Addr:            kafkaAddrs,
			BatchSize:       1,
			WriteBackoffMin: 1 * time.Millisecond,
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
		traceID := v.ID.Hex() + "sync_by_cli"
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
	topic := fmt.Sprintf("%s.%s.operate", "operational-materials-svc", "category")
	headers := map[string]string{
		"scope": scope,
		"env":   env,
	}
	msg := []event.Event{}
	for _, v := range datas {
		traceID := v.ID.Hex() + "sync_by_cli"
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
	topic := fmt.Sprintf("%s.%s.operate", "operational-materials-svc", "position")
	headers := map[string]string{
		"scope": scope,
		"env":   env,
	}
	msg := []event.Event{}
	for _, v := range datas {
		traceID := v.ID.Hex() + "sync_by_cli"
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
		traceID := v.ID.Hex() + "sync_by_cli"
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

func SendOperitionPositionCreateMesssage(ctx context.Context, mq event.Sender, scope, env string, datas []*H5PropertiesWithActName) error {
	topic := fmt.Sprintf("%s.%s.operate", "operational-h5-svc", "properties")
	headers := map[string]string{
		"scope": scope,
		"env":   env,
	}
	msg := []event.Event{}
	for _, v := range datas {
		traceID := v.ID + "sync_by_cli"
		fmt.Println("traceID", traceID)
		oe := &h5api.H5PropertiesOperateEvent{
			Data: []*h5api.H5PropertiesOperateEvent_H5PropertiesWithName{
				{
					H5Properties: &h5api.H5Properties{
						Id:         v.ID,
						Attribute:  v.Attribute,
						Style:      v.Style,
						ActivityID: v.ActID,
					},
					Name: v.ActName,
				},
			},
			OperateType: "Create",
			OperatedAt:  time.Now().Unix(),
		}
		data, err := protojson.Marshal(oe)
		if err != nil {
			return err
		}

		msg = append(msg, event.NewEvent(
			data,
			v.ID,
			event.WithHeaders(headers),
			event.WithTopic(topic),
			event.WithHeaderTrackID(traceID),
		))
	}

	return mq.Send(ctx, msg...)
}
