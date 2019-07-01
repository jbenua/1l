package worker

import (
	"1link"
	"encoding/csv"
	"errors"
	"memcache"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/DenisCheremisov/gosnippets/golog"
	"github.com/bradfitz/gomemcache/memcache"
)

const apiURL = "http://icq.com/admin/get1link?id="

func getURL(str string) string {
	return apiURL + url.QueryEscape(str)
}

// Referer get references, incorporates caching utilities inside
type Referer struct {
	cacher memcch.Memcache
}

// NewReferer constructs Referer
func NewReferer(cacher memcch.Memcache) *Referer {
	return &Referer{
		cacher: cacher,
	}
}

// Get get stored reference if available
func (r *Referer) Get(key string) (string, bool, error) {

	key = strconv.QuoteToASCII(key)
	item, err := r.cacher.Get(key)
	if err == memcache.ErrCacheMiss {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}
	if string(item.Value) == "null" {
		err = r.cacher.Delete(key)
		defer func() {
			if err != nil {
				log.Warning(err)
			}
		}()
		return "", false, nil
	}

	return string(item.Value), true, nil
}

// Set set stored reference if available
func (r *Referer) Set(key string, value string) error {
	if value != "null" && value != "" {
		key = strconv.QuoteToASCII(key)
		in2Months := time.Now().AddDate(0, 2, 0).Unix()
		item := &memcache.Item{
			Key:        key,
			Value:      []byte(value),
			Expiration: int32(in2Months),
		}
		log.Infof("trying to set {%v: %v}....\n", key, value)
		err := r.cacher.Set(item)
		if err == nil {
			log.Infof("{%v: %v} successfully saved\n", key, value)
		}
		return err
	}
	return errors.New("Null or empty referrer won't be saved")
}

// GetReferers get referers for ID131 and ID133
func GetReferers(
	item *onelink.Parsed,
	filter onelink.FilterResult,
	requester *Requester,
	referer *Referer) (first string, last string, err error) {

	var key string
	var firstKey string
	var ok bool

	// First referer
	if filter.ID131 != "" {
		key := item.Stats.SystemInfo[0].ProtocolUID + "::" + filter.ID131
		firstKey = key
		first, ok, err = referer.Get(key)
		if err != nil {
			return
		}
		if !ok {
			first, _ = requester.Get1Link(filter.ID131)
		}
		err = referer.Set(key, first)
		if err != nil {
			return
		}
	}

	// Last referer
	key = item.Stats.SystemInfo[0].ProtocolUID + "::" + filter.ID133
	if filter.ID133 != "" && key != firstKey {
		last, ok, err = referer.Get(key)
		if err != nil {
			return
		}
		if !ok {
			last, _ = requester.Get1Link(filter.ID133)
		}
		err = referer.Set(key, last)
		if err != nil {
			return
		}
	} else if key == firstKey {
		last = first
	}
	return first, last, nil
}

// Worker worker
func Worker(
	writer *SyncWriter,
	parsedChannel chan (*onelink.Parsed),
	requester *Requester,
	wg *sync.WaitGroup,
	referer *Referer) {

	output := csv.NewWriter(writer.GetWriter())
	output.Comma = '|'
	raw := make([]string, 11)

	for {
		item := <-parsedChannel
		if item == nil {
			log.Info("Stopping")
			wg.Done()
			return
		}

		filter, err := item.OneLinkFilter()

		if err != nil {
			log.Error(err)
			continue
		}
		if !filter.Ok {
			continue
		}

		firstReferer, lastReferer, err := GetReferers(item, filter, requester, referer)
		if err != nil {
			log.Warning(err)
		}

		// Prepare buffer
		raw[0] = item.Dtime.Format(time.RFC3339)
		raw[1] = item.IP
		raw[2] = item.Stats.SystemInfo[0].AccountType
		raw[3] = item.Stats.UserAgent.Platform
		raw[4] = filter.DeviceID
		raw[5] = item.Stats.SystemInfo[0].ProtocolUID
		raw[6] = item.Stats.UserAgent.Name
		raw[7] = filter.ID131
		raw[8] = firstReferer
		raw[9] = filter.ID133
		raw[10] = lastReferer

		writer.Lock()
		err = output.Write(raw)
		if err != nil {
			log.Error(err)
			continue
		}
		writer.Unlock()
	}
}
