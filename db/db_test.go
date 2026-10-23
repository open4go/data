package db

import (
	"context"
	"errors"
	"testing"

	v9 "github.com/redis/go-redis/v9"
)

func TestGetRedisDataHandlerMissingDoesNotExit(t *testing.T) {
	old := getHandler
	getHandler = func(string) (*v9.Client, error) { return nil, errors.New("no found any handler") }
	t.Cleanup(func() { getHandler = old })
	if GetRedisDataHandler(context.Background()) != nil {
		t.Fatal("want nil client")
	}
}
