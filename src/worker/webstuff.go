package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/DenisCheremisov/gosnippets/golog"
)

// RawRequester ...
type RawRequester interface {
	Get1Link(string) ([]byte, error)
}

// HTTPClient abstraction
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// RawRequesterImpl 1link raw requester implementation
type RawRequesterImpl struct {
	tr     *http.Transport
	client *http.Client
}

// NewRawRequesterImpl constructor
func NewRawRequesterImpl() *RawRequesterImpl {
	tr := &http.Transport{}
	return &RawRequesterImpl{
		tr:     tr,
		client: &http.Client{Timeout: time.Duration(5 * time.Second), Transport: tr}}
}

// Get1Link implements Requester for RawRequester
func (rr *RawRequesterImpl) Get1Link(val string) (res []byte, err error) {
	res = nil
	req, err := http.NewRequest("GET", apiURL+url.QueryEscape(val), nil)

	if err != nil {
		return
	}

	resp, err := rr.client.Do(req)
	if err != nil {
		return
	}
	if resp.StatusCode != 200 {
		err = fmt.Errorf("Failed to retrieve 1link info, HTTP status code %d", resp.StatusCode)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Requester requester
type Requester struct {
	rawRequester RawRequester
}

// NewRequester constructor
func NewRequester(rr RawRequester) *Requester {
	return &Requester{
		rawRequester: rr,
	}
}

// Get1Link value
func (r *Requester) Get1Link(key string) (string, bool) {
	var err error
	defer func() {
		if err != nil {
			log.Error(err)
		}
	}()

	body, err := r.rawRequester.Get1Link(key)
	if err != nil {
		return "", false
	}

	decoder := json.NewDecoder(bytes.NewBuffer(body))
	var respData struct {
		Status     int              `json:"status"`
		StatusText string           `json:"statusText"`
		Data       *json.RawMessage `json:"data,omitempty"`
	}
	err = decoder.Decode(&respData)
	if err != nil {
		err = fmt.Errorf("JSON decoder cannot parse %s: %s", string(body), err.Error())
		return "", false
	}

	if respData.Status != 200 {
		return respData.StatusText, true
	} else if respData.Data == nil {
		log.Warningf("Got no data for %s%s", apiURL, url.QueryEscape(key))
		return "", true
	}
	return string(*respData.Data), true
}
