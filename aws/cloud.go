package aws

import (
	"context"
	"log"
	"time"

	"github.com/spf13/viper"
	"github.com/xid-protocol/xidp/protocols"
	"github.com/xid-protocol/xidp/xdb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type AWSCloud struct {
	Ctx      context.Context
	DBClient *xdb.Client
}

func NewAWSCloud() *AWSCloud {
	ctx := context.Background()

	// 1) 连接 Mongo 并获取集合
	mongoURI := viper.GetString("mongodb.uri")
	mc, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal(err)
	}
	db := mc.Database(viper.GetString("mongodb.database"))
	col := db.Collection("aws_info")

	client := xdb.NewClientWithMongo(col, &xdb.ClientOptions{
		EnableIdempotency: true,
		DefaultTimeout:    2 * time.Second,
	})
	return &AWSCloud{
		Ctx:      ctx,
		DBClient: client,
	}
}

func (c *AWSCloud) GetAllEC2Info() []*protocols.XID {

	q := xdb.Query{
		PageSize: 100,
		SortBy:   "createdAt",
		SortAsc:  false,
	}
	total, err := c.DBClient.Count(c.Ctx, q)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("total: %d", total)

	// 3) 分页拉取直到拿完
	result := make([]*protocols.XID, 0)
	for {
		items, next, err := c.DBClient.List(c.Ctx, q)
		if err != nil {
			log.Fatal(err)
			break
		}
		// 使用 items（每个 xid 只有最新一条）
		result = append(result, items...)

		if next == "" {
			break // 没有下一页
		}
		q.AfterCursor = &next // 继续下一页
	}
	return result

}
