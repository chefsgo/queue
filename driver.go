package queue

type (
	// Driver 数据驱动
	Driver interface {
		Connect(name string, config Config) (Connect, error)
	}
	Health struct {
		Workload int64
	}

	Delegate interface {
		Serve(string, []byte)
	}

	// Connect 连接
	Connect interface {
		Open() error
		Health() (Health, error)
		Close() error

		Accept(Delegate) error
		Register(name string) error

		Start() error

		Enqueue(name string, data []byte) error
	}
)
