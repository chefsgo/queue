package queue

import (
	"strings"

	. "github.com/chefsgo/base"
	"github.com/chefsgo/chef"
)

func (this *Instance) newContext() *Context {
	return &Context{inst: this}
}

// 收到消息
// 待优化，加入协程池，限制单个队列的并发
func (this *Instance) Serve(alias string, data []byte) {
	if strings.HasPrefix(alias, this.config.Prefix) {
		alias = strings.TrimPrefix(alias, this.config.Prefix)
	}

	ctx := this.newContext()

	// 解析过来的数据
	meta := chef.Meta{}
	err := chef.Unmarshal(this.config.Codec, data, &meta)
	if err == nil {
		ctx.Context = chef.NewContext(meta)
	} else {
		ctx.Context = chef.NewContext()
	}

	//名称和别名
	ctx.Alias = alias
	relate := this.module.relateKey(this.name, alias)
	if name, ok := this.module.relates[relate]; ok {
		ctx.Name = name
		if cfg, ok := this.module.queues[ctx.Name]; ok {
			ctx.Config = &cfg
		}
	}

	//开始执行
	this.request(ctx)
	// response 是最后的反馈，是必须执行的
	this.response(ctx)
}

// request 请求处理
func (this *Instance) request(ctx *Context) {
	ctx.clear()

	//处理是否存在

	//request拦截器
	ctx.next(this.module.requestFilters...)
	ctx.next(this.finding)     //存在否
	ctx.next(this.authorizing) //身份验证
	ctx.next(this.arguing)     //参数处理
	ctx.next(this.execute)

	//开始执行
	ctx.Next()
}

// finding 判断不
func (this *Instance) finding(ctx *Context) {
	if ctx.Config == nil {
		ctx.Found()
	} else {
		ctx.Next()
	}
}

// authorizing token验证
func (this *Instance) authorizing(ctx *Context) {
	// 待处理
	ctx.Next()
}

// arguing 参数解析
func (this *Instance) arguing(ctx *Context) {
	if ctx.Config.Args != nil {
		argsValue := Map{}
		res := chef.Mapping(ctx.Config.Args, ctx.Value, argsValue, ctx.Config.Nullable, false, ctx.Context)
		if res != nil && res.Fail() {
			ctx.Failed(res)
		}

		for k, v := range argsValue {
			ctx.Args[k] = v
		}
	}
	ctx.Next()
}

func (this *Instance) found(ctx *Context) {
	ctx.clear()

	//把失败处理器加入调用列表
	ctx.next(this.module.foundHandlers...)

	ctx.Next()
}

func (this *Instance) error(ctx *Context) {
	ctx.clear()

	//把失败处理器加入调用列表
	ctx.next(this.module.errorHandlers...)

	ctx.Next()
}

func (this *Instance) failed(ctx *Context) {
	ctx.clear()

	//把失败处理器加入调用列表
	ctx.next(this.module.failedHandlers...)

	ctx.Next()
}

func (this *Instance) denied(ctx *Context) {
	ctx.clear()

	//把失败处理器加入调用列表
	ctx.next(this.module.deniedHandlers...)

	ctx.Next()
}

// execute 执行线
func (this *Instance) execute(ctx *Context) {
	ctx.clear()

	//execute拦截器
	ctx.next(this.module.executeFilters...)
	if ctx.Config.Action != nil {
		ctx.next(ctx.Config.Action)
	}

	//开始执行
	ctx.Next()
}

// response 响应线
func (this *Instance) response(ctx *Context) {
	ctx.clear()

	//response拦截器
	ctx.next(this.module.responseFilters...)
	ctx.next(this.body)

	//开始执行
	ctx.Next()
}

// 最终的默认body响应
func (this *Instance) body(ctx *Context) {
	switch body := ctx.Body.(type) {
	case retryBody:
		this.bodyRetry(ctx, body)
	default:
		this.bodyDefault(ctx)
	}
}

// bodyRetry 默认的body处理
func (this *Instance) bodyRetry(ctx *Context, body retryBody) {

	if ctx.Config.Retry > 0 {
		if ctx.Retries() < ctx.Config.Retry {
			meta := ctx.Meta()
			meta.Retries++
			ctx.inst.module.publishTo(ctx.inst.name, ctx.Name, meta)
		}
	}
}

// bodyDefault 默认的body处理
func (this *Instance) bodyDefault(ctx *Context) {
	//here is nothing todo
}
