package queue

import (
	"fmt"

	. "github.com/chefsgo/base"
)

type (
	Queue struct {
		Name     string   `json:"name"`
		Text     string   `json:"text"`
		Alias    []string `json:"alias"`
		Nullable bool     `json:"-"`
		Args     Vars     `json:"args"`
		Setting  Map      `json:"-"`
		Coding   bool     `json:"-"`

		Action ctxFunc `json:"-"`

		// 待优化，队列是不是需要做 token 验证
		// 但是考虑到统一性，最好是加上
		// 因为也许 method 里面是要加的，这样方便直接在method获取用户身份什么的
		// Token bool `json:"token"`
		// Auth  bool `json:"auth"`

		Connect string `json:"connect"`
		Retry   int    `json:"retry"`
	}

	// Notice 通知，表示当前节点会发出的队列预告
	// 比如，支付模块，可能会发布 pay.Finish 之类的一系列的支付完成的队列
	// 在集群模式下，应该会把节点的notice写入集群节点信息下
	// 这样方便，生成分布式的文档
	Notice struct {
		Name     string `json:"name"`
		Text     string `json:"text"`
		Nullable bool   `json:"nullable"`
		Args     Vars   `json:"args"`
	}

	// Filter 拦截器
	Filter struct {
		Name     string  `json:"name"`
		Text     string  `json:"text"`
		Request  ctxFunc `json:"-"`
		Execute  ctxFunc `json:"-"`
		Response ctxFunc `json:"-"`
	}
	// Handler 处理器
	Handler struct {
		Name   string  `json:"name"`
		Text   string  `json:"text"`
		Found  ctxFunc `json:"-"`
		Error  ctxFunc `json:"-"`
		Failed ctxFunc `json:"-"`
		Denied ctxFunc `json:"-"`
	}
)

func (module *Module) Queue(name string, config Queue, override bool) {
	if config.Alias == nil || len(config.Alias) == 0 {
		config.Alias = []string{name}
	}

	if _, ok := module.queues[name]; ok {
		panic(fmt.Sprintf("Queue %s already registered.", name))
	} else {
		module.queues[name] = config
	}
}

// Notice 注册 通知
func (module *Module) Notice(name string, config Notice, override bool) {
	if _, ok := module.notices[name]; ok {
		panic(fmt.Sprintf("Notice %s already registered.", name))
	} else {
		module.notices[name] = config
	}
}

// Filter 注册 拦截器
func (module *Module) Filter(name string, config Filter, override bool) {
	if override {
		module.filters[name] = config
	} else {
		if _, ok := module.filters[name]; ok == false {
			module.filters[name] = config
		}
	}
}

// Handler 注册 处理器
func (module *Module) Handler(name string, config Handler, override bool) {
	if override {
		module.handlers[name] = config
	} else {
		if _, ok := module.handlers[name]; ok == false {
			module.handlers[name] = config
		}
	}
}
