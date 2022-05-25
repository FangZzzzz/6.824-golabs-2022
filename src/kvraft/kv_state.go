package kvraft

type KvState struct {
	Kv map[string]string
}

func NewKvState() *KvState {
	return &KvState{
		Kv: map[string]string{},
	}
}

func (k *KvState) Get(key string) (string, Err) {
	value, ok := k.Kv[key]
	if !ok {
		return "", ErrNoKey
	}
	return value, OK
}

func (k *KvState) Put(key, value string) {
	k.Kv[key] = value
}

func (k *KvState) Append(key, value string) {
	k.Kv[key] += value
}
