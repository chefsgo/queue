package queue

import (
	"fmt"

	. "github.com/chefsgo/base"
	"github.com/chefsgo/chef"
)

// Enqueue 消息入队
func (this *Module) enqueueTo(connect, name string, meta chef.Metadata) error {
	locate := this.hashring.Locate(name)

	if inst, ok := this.instances[locate]; ok {

		// 看看是不是有 notice 定义，如果有，并有args定义，要处理参数包装
		if meta.Payload != nil {
			if notice, ok := this.notices[name]; ok {
				if notice.Args != nil {
					value := Map{}
					res := chef.Mapping(notice.Args, meta.Payload, value, notice.Nullable, false)
					if res == nil || res.OK() {
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
	locate := this.hashring.Locate(name)
	return this.enqueueTo(locate, name, meta)
}

// Publish 发起消息
func (this *Module) Enqueue(name string, values ...Map) error {
	//原数据
	var payload Map
	if len(values) > 0 {
		payload = values[0]
	}
	meta := chef.Metadata{Name: name, Payload: payload}

	return this.enqueue(name, meta)
}
