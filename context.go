package queue

import (
	. "github.com/chefsgo/base"
	"github.com/chefsgo/chef"
)

type (
	Context struct {
		inst *Instance
		*chef.Context

		index int       //下一个索引
		nexts []ctxFunc //方法列表

		// 以下几个字段必须独立
		// 要不然，Invoke的时候，会被修改掉
		Name    string
		Alias   string
		Config  *Queue
		Value   Map
		Args    Map
		Setting Map

		Body Any
	}
	ctxFunc func(*Context)

	//重试
	retryBody = struct{}
)

func (ctx *Context) clear() {
	ctx.index = 0
	ctx.nexts = make([]ctxFunc, 0)
}
func (ctx *Context) next(nexts ...ctxFunc) {
	ctx.nexts = append(ctx.nexts, nexts...)
}

func (ctx *Context) Next() {
	if len(ctx.nexts) > ctx.index {
		next := ctx.nexts[ctx.index]
		ctx.index++
		if next != nil {
			next(ctx)
		} else {
			ctx.Next()
		}
	}
}

func (ctx *Context) Found() {
	ctx.inst.found(ctx)
}
func (ctx *Context) Erred(res Res) {
	ctx.Result(res)
	ctx.inst.error(ctx)
}
func (ctx *Context) Failed(res Res) {
	ctx.Result(res)
	ctx.inst.failed(ctx)
}
func (ctx *Context) Denied(res Res) {
	ctx.Result(res)
	ctx.inst.denied(ctx)
}

// 返回重试
func (ctx *Context) Retry() {
	ctx.Body = retryBody{}
}
