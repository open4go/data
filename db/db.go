package db

import (
	"context"
	r2redis "github.com/open4go/db/redis"
	"github.com/open4go/log"
	v9 "github.com/redis/go-redis/v9"
)

var getHandler = func(name string) (*v9.Client, error) { return r2redis.DBPool.GetHandler(name) }

// GetRedisDataHandler 获取数据库handler 这里定义一个方法。
// 未配置 data redis 时记录错误并返回 nil（不退出进程），调用方需判断 nil。
func GetRedisDataHandler(ctx context.Context) *v9.Client {
	handler, err := getHandler("data")
	if err != nil {
		log.Log(ctx).WithField("db", "data").Error(err)
		return nil
	}
	return handler
}
