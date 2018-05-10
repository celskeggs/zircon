package util

type MultiTeardown struct {
	teardowns []func()
}

func (m *MultiTeardown) Teardown() {
	for _, teardown := range m.teardowns {
		teardown()
	}
}

func (m *MultiTeardown) Add(teardowns ...func()) {
	m.teardowns = append(m.teardowns, teardowns...)
}
