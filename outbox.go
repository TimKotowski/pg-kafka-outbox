package outbox

type Outbox interface {
	Init() error
}

type outbox struct {
	config *Config
}

func NewOutbox(config *Config) Outbox {
	return &outbox{
		config: config,
	}
}

func (o *outbox) Init() error {
	return nil
}
