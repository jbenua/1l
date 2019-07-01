package worker

import (
	"1link"
	"encoding/xml"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/stretchr/testify/assert"
)

type Dummy1LinkClient string

func (c Dummy1LinkClient) Get1Link(val string) ([]byte, error) {
	return []byte(c), nil
}

type Dummy1LinkKeyClient int

func (c Dummy1LinkKeyClient) Get1Link(val string) ([]byte, error) {
	return []byte("{\"status\":200,\"statusText\":\"\",\"data\":\"requester::" + val + "\"}"), nil
}

type DummyMemcache map[string]memcache.Item

func NewDummyMemcache() *DummyMemcache {
	res := DummyMemcache(make(map[string]memcache.Item))
	return &res
}

func (dm *DummyMemcache) Get(key string) (*memcache.Item, error) {
	res, ok := (*dm)[key]

	if !ok {
		return nil, memcache.ErrCacheMiss
	}
	return &res, nil
}

func (dm *DummyMemcache) Set(item *memcache.Item) error {
	(*dm)[item.Key] = *item
	return nil
}

func (dm *DummyMemcache) Delete(key string) error {
	delete(*dm, key)
	return nil
}

func TestGetReferers(t *testing.T) {
	item := &onelink.Parsed{
		Line:  "",
		IP:    "127.0.0.1",
		Dtime: time.Now(),
		Stats: onelink.Stats{
			Tag: xml.Name{Space: "tag-space", Local: "local-space"},
			UserAgent: onelink.UserAgent{
				Name:     "ua-name",
				Protocol: "ua-protocol",
				Lang:     "ua-lang",
				Platform: "ua-platform",
			},
			SystemInfo: []onelink.SystemInfo{
				onelink.SystemInfo{
					Login:       "sysinfo-login",
					ProtocolUID: "icq",
					Server:      "sysinfo-server",
					AccountType: "sysinfo-account-type",
					Main:        "sysinfo-main",
					Appendix: []onelink.Appendix{
						onelink.Appendix{
							Tag: "appendix-tag",
							ID:  "appendix-id",
							Data: onelink.Collection(map[string]string{
								"131": "set_abcdefff",
								"133": "set_1link_12345678",
								"44":  "fghjdgjdhfkj",
							}),
						},
					},
				},
			},
		},
	}
	filter, err := item.OneLinkFilter()
	if err != nil {
		t.Fatal(err)
	}

	reqster := NewRequester(Dummy1LinkKeyClient(1))
	mc := NewDummyMemcache()
	referer := NewReferer(mc)

	f, l, err := GetReferers(item, filter, reqster, referer)
	if err != nil {
		t.Fatal(err)
	}

	if !assert.Equal(t, "\"requester::abcdefff\"", f) {
		return
	}

	if !assert.Equal(t, "\"requester::1link_12345678\"", l) {
		return
	}
}

type StringRequester string

func (dr StringRequester) Get1Link(key string) (string, bool) {
	return string(dr), true
}

func TestWithNullHttpResponse(t *testing.T) {
	item := &onelink.Parsed{
		Line:  "",
		IP:    "127.0.0.1",
		Dtime: time.Now(),
		Stats: onelink.Stats{
			Tag: xml.Name{Space: "tag-space", Local: "local-space"},
			UserAgent: onelink.UserAgent{
				Name:     "ua-name",
				Protocol: "ua-protocol",
				Lang:     "ua-lang",
				Platform: "ua-platform",
			},
			SystemInfo: []onelink.SystemInfo{
				onelink.SystemInfo{
					Login:       "sysinfo-login",
					ProtocolUID: "icq",
					Server:      "sysinfo-server",
					AccountType: "sysinfo-account-type",
					Main:        "sysinfo-main",
					Appendix: []onelink.Appendix{
						onelink.Appendix{
							Tag: "appendix-tag",
							ID:  "appendix-id",
							Data: onelink.Collection(map[string]string{
								"131": "set_abcdefff",
								"133": "set_1link_12345678",
								"44":  "fghjdgjdhfkj",
							}),
						},
					},
				},
			},
		},
	}
	filter, err := item.OneLinkFilter()
	if err != nil {
		t.Fatal(err)
	}

	reqster := NewRequester(Dummy1LinkClient("{\"status\":200,\"statusText\":\"\",\"data\":null}"))
	mc := NewDummyMemcache()
	referer := NewReferer(mc)

	f, l, err := GetReferers(item, filter, reqster, referer)
	if err == nil {
		t.Fatal(err)
	}
	if !assert.Equal(t, "", f, l) {
		return
	}

	reqster = NewRequester(Dummy1LinkClient("{\"status\":200,\"statusText\":\"\",\"data\":{\"a\":1}}"))
	mc = NewDummyMemcache()
	referer = NewReferer(mc)
	f, l, err = GetReferers(item, filter, reqster, referer)
	if err != nil {
		t.Fatal(err)
	}
	if !assert.Equal(t, "{\"a\":1}", f, l) {
		return
	}
}

func TestSetNull(t *testing.T) {
	fmt.Println("\n=========\nSet test\n=========")
	mc := NewDummyMemcache()
	key := "my-awesome-key"
	referer := NewReferer(mc)

	res, ok, err := referer.Get(key)
	if err != nil && res != "" && ok {
		t.Fatal("Key ", key, "occured in DB")
		return
	}
	err = referer.Set(key, "null")
	if err == nil {
		t.Fatal("Saved null referrer")
		return
	}
	res, ok, err = referer.Get(key)
	if err != nil && res != "" && ok {
		t.Fatal("Key ", key, "occured in DB")
		return
	}
}

func TestDeleteNullsFromDB(t *testing.T) {

	fmt.Printf("\n=========\nDelete test\n=========")
	mc := NewDummyMemcache()
	key := "my-awesome-key"
	referer := NewReferer(mc)
	err := mc.Set(&memcache.Item{Key: strconv.QuoteToASCII(key), Value: []byte("null")})
	item, _, err := referer.Get(key)
	if !assert.Equal(t, "", item) {
		t.Error("Should not show this item")
		return
	}
	_, err = mc.Get(strconv.QuoteToASCII(key))
	if err == nil {
		t.Fatal("Key ", key, "occured in DB")
		return
	}
}
