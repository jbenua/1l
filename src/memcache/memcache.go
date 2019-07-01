package memcch

import "github.com/bradfitz/gomemcache/memcache"

// Memcache abstraction layer
type Memcache interface {
	Get(key string) (*memcache.Item, error)
	Set(item *memcache.Item) error
	Delete(key string) error
}

// NamespacedMemcache namespaced cacher
type NamespacedMemcache struct {
	prefix string
	client Memcache
}

// NewNamespacedMemcache constructor
func NewNamespacedMemcache(prefix string, client Memcache) *NamespacedMemcache {
	return &NamespacedMemcache{
		prefix: prefix,
		client: client,
	}
}

func (nm *NamespacedMemcache) name(key string) string {
	return nm.prefix + "::" + key
}

// Get implementation
func (nm *NamespacedMemcache) Get(key string) (*memcache.Item, error) {
	res, err := nm.client.Get(nm.name(key))
	return res, err
}

// Set implementation
func (nm *NamespacedMemcache) Set(item *memcache.Item) error {
	newItem := memcache.Item{
		Expiration: item.Expiration,
		Flags:      item.Flags,
		Key:        nm.name(item.Key),
		Value:      item.Value,
	}
	err := nm.client.Set(&newItem)
	return err
}

// Delete implementation
func (nm *NamespacedMemcache) Delete(key string) error {
	err := nm.client.Delete(nm.name(key))
	return err
}
