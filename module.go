package queue

import (
	"fmt"
	"sync"

	. "github.com/chefsgo/base"
	"github.com/chefsgo/chef"
	"github.com/chefsgo/util"
)

func init() {
	chef.Register(NAME, module)
}

var (
	module = &Module{
		configs:   make(map[string]Config, 0),
		drivers:   make(map[string]Driver, 0),
		instances: make(map[string]Instance, 0),

		queues:   make(map[string]Queue, 0),
		notices:  make(map[string]Notice, 0),
		filters:  make(map[string]Filter, 0),
		handlers: make(map[string]Handler, 0),

		relates: make(map[string]string, 0),
	}
)

type (
	Module struct {
		mutex sync.Mutex

		connected, initialized, launched bool

		configs map[string]Config
		drivers map[string]Driver

		queues   map[string]Queue
		notices  map[string]Notice
		filters  map[string]Filter
		handlers map[string]Handler

		relates map[string]string

		requestFilters  []ctxFunc
		executeFilters  []ctxFunc
		responseFilters []ctxFunc

		foundHandlers  []ctxFunc
		errorHandlers  []ctxFunc
		failedHandlers []ctxFunc
		deniedHandlers []ctxFunc

		instances map[string]Instance

		weights  map[string]int
		hashring *util.HashRing
	}

	Config struct {
		Driver  string
		Codec   string
		Weight  int
		Prefix  string
		Setting Map
	}
	Instance struct {
		module  *Module
		name    string
		config  Config
		connect Connect
	}
)

// Enqueue 消息入队
func (this *Module) enqueueTo(connect, name string, meta chef.Metadata) error {
	locate := module.hashring.Locate(name)

	if inst, ok := module.instances[locate]; ok {

		// 看看是不是有 notice 定义，如果有，并有args定义，要处理参数包装
		if meta.Payload != nil {
			if notice, ok := this.notices[name]; ok {
				if notice.Args != nil {
					value := Map{}
					res := chef.Mapping(notice.Args, meta.Payload, value, notice.Nullable, false)
					if res.OK() {
						meta.Payload = value
					}
				}
			}
		}

		//原数据
		bytes, err := chef.Marshal(inst.config.Codec, &meta)
		if err != nil {
			return err
		}

		name := inst.config.Prefix + name
		return inst.connect.Enqueue(name, bytes)
	}

	return errInvalidConnection
}

func (this *Module) relateKey(conn, alias string) string {
	return fmt.Sprintf("%s-%s")
}

func (this *Module) enqueue(name string, meta chef.Metadata) error {
	locate := module.hashring.Locate(name)
	return this.enqueueTo(locate, name, meta)
}
