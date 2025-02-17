package ipconf

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/hardcore-os/plato/ipconf/domain"
)

type Response struct {
	Message string      `json:"message"`
	Code    int         `json:"code"`
	Data    interface{} `json:"data"`
}

// GetIpInfoList API 适配应用层
func GetIpInfoList(c context.Context, ctx *app.RequestContext) {
	// Hertz 框架自动将请求相关的信息封装到c和ctx
	defer func() { // 以防一次错误调用，导致整个服务崩溃
		if err := recover(); err != nil { // recover() 用于捕获异常。当发生 panic 时，recover() 能够拦截异常并返回错误值。
			ctx.JSON(consts.StatusBadRequest, utils.H{"err": err})
		}
	}()
	// Step0: 构建客户请求信息
	ipConfCtx := domain.BuildIpConfContext(&c, ctx)
	// Step1: 进行ip调度
	eds := domain.Dispatch(ipConfCtx)
	// Step2: 根据得分取top5返回
	ipConfCtx.AppCtx.JSON(consts.StatusOK, packRes(top5Endports(eds)))
}
