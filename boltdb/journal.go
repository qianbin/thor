package boltdb

type action struct {
	key   []byte
	value *[]byte
}

type entry struct {
	value   []byte
	deleted bool
}

type journal struct {
	state   map[string]*entry
	actions []*action
}

func newJournal() *journal {
	return &journal{state: make(map[string]*entry)}
}
func (j *journal) Put(key, value []byte) {
	key = append([]byte(nil), key...)
	value = append([]byte(nil), value...)
	j.actions = append(j.actions, &action{key, &value})
	j.state[string(key)] = &entry{value: value}
}

func (j *journal) Delete(key []byte) {
	key = append([]byte(nil), key...)
	j.actions = append(j.actions, &action{key, nil})
	j.state[string(key)] = &entry{deleted: true}
}

func (j *journal) Get(key []byte) *entry {
	return j.state[string(key)]
}

func (j *journal) Len() int {
	return len(j.actions)
}
