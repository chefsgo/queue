package queue

import (
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
		filters:  make(map[string]Filter, 0),
		handlers: make(map[string]Handler, 0),
	}
)

type (
	Module struct {
		mutex sync.Mutex

		connected, initialized, launched bool

		configs map[string]Config
		drivers map[string]Driver

		queues   map[string]Queue
		filters  map[string]Filter
		handlers map[string]Handler

		serveFilters    []ctxFunc
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

// Publish 发起消息
func (this *Module) publishTo(connect, name string, meta chef.Meta) error {
	locate := module.hashring.Locate(name)

	if inst, ok := module.instances[locate]; ok {
		//原数据
		bytes, err := chef.Marshal(inst.config.Codec, &meta)
		if err != nil {
			return err
		}

		name := inst.config.Prefix + name
		return inst.connect.Publish(name, bytes)
	}

	return errInvalidConnection
}

func (this *Module) publish(name string, meta chef.Meta) error {
	locate := module.hashring.Locate(name)
	return this.publishTo(locate, name, meta)
}
