package queue

import (
	. "github.com/chefsgo/base"
)

type (
	Queue struct {
		Name     string   `json:"name"`
		Text     string   `json:"text"`
		Alias    []string `json:"-"`
		Nullable bool     `json:"-"`
		Args     Vars     `json:"args"`
		Data     Vars     `json:"data"`
		Setting  Map      `json:"-"`
		Coding   bool     `json:"-"`

		Action ctxFunc `json:"-"`

		// 待优化，队列是不是需要做 token 验证
		// 但是考虑到统一性，最好是加上
		// 因为也许 method 里面是要加的，这样方便直接在method获取用户身份什么的
		// Token bool `json:"token"`
		// Auth  bool `json:"auth"`

		Assign string `json:"assign"`
		Retry  int    `json:"retry"`
	}

	Filter struct {
		Name     string  `json:"name"`
		Text     string  `json:"text"`
		Serve    ctxFunc `json:"-"`
		Request  ctxFunc `json:"-"`
		Execute  ctxFunc `json:"-"`
		Response ctxFunc `json:"-"`
	}
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

	// if config.Method == "" {
	// 	config.Method = name
	// }

	// //需要代注册方法
	// if config.Action != nil {
	// 	method := Method{"", 0, 0, config.Name, config.Desc, config.Alias, config.Nullable, config.Args, config.Data, config.Setting, config.Coding, config.Action, config.Token, config.Auth}
	// 	mEngine.Method(config.Method, method, overrides...)
	// }

	alias := make([]string, 0)
	if name != "" {
		alias = append(alias, name)
	}
	if config.Alias != nil {
		alias = append(alias, config.Alias...)
	}

	for _, key := range alias {
		if override {
			module.queues[key] = config
		} else {
			if _, ok := module.queues[key]; ok == false {
				module.queues[key] = config
			}
		}
	}
}

// Filter 拦截器
func (module *Module) Filter(name string, config Filter, override bool) {
	if override {
		module.filters[name] = config
	} else {
		if _, ok := module.filters[name]; ok == false {
			module.filters[name] = config
		}
	}
}

// Handler 处理器
func (module *Module) Handler(name string, config Handler, override bool) {
	if override {
		module.handlers[name] = config
	} else {
		if _, ok := module.handlers[name]; ok == false {
			module.handlers[name] = config
		}
	}
}
