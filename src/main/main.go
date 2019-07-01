package main

import (
	"1link"
	"bufio"
	"flag"
	"memcache"
	"time"
	"worker"

	"github.com/DenisCheremisov/gosnippets/golog"
	"github.com/bradfitz/gomemcache/memcache"

	"config"
	"os"
	"sync"
)

const (
	workersNo                = 50
	oneLinkURL               = "http://host.com/get1link?id="
	onelinkMemcacheNamespace = "1link"
)

var parsedChannel chan (*onelink.Parsed)
var wg sync.WaitGroup

func main() {
	cfgFileName := flag.String("config", "db_cfg.yaml", "Config file name")
	flag.Parse()

	// Config
	cfg, err := config.ReadConfig(*cfgFileName)
	if err != nil {
		log.Fatal(err)
	}

	// Initialization
	parsedChannel := make(chan *onelink.Parsed)
	wg := &sync.WaitGroup{}
	syncWriter := worker.NewSyncWriter(os.Stdout)

	// Initialize referer object
	mcRaw := memcache.New(cfg.DB.Host + ":" + cfg.DB.Port)
	mcRaw.Timeout = 5000 * time.Millisecond
	mc := memcch.NewNamespacedMemcache(onelinkMemcacheNamespace, mcRaw)
	referer := worker.NewReferer(mc)

	httpClient := worker.NewRawRequesterImpl()

	// Starting workers
	wg.Add(workersNo)
	for i := 0; i < workersNo; i++ {
		go worker.Worker(syncWriter, parsedChannel, worker.NewRequester(httpClient), wg, referer)
	}

	defer func() {
		for i := 0; i < workersNo; i++ {
			parsedChannel <- nil
		}
		wg.Wait()
		syncWriter.Finish()
	}()

	// Start reading
	reader := bufio.NewReaderSize(os.Stdin, 512*1024)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		data := scanner.Bytes()
		parsed, ok := onelink.LineParse(data)
		if ok {
			parsedChannel <- parsed
		}
	}
	log.Info("Finished the stream")
}
