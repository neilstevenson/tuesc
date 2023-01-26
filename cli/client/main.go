package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

const loggingLevel = logger.InfoLevel
const MAX = 1_000_000
const SLOW_THRESHOLD_NANOS = 500_000

type Tuple2 struct {
	F0 string
	F1 string
}

func getClient(ctx context.Context, my_host string) *hazelcast.Client {
	config := hazelcast.Config{}
	config.ClientName = "neil-go"
	config.Cluster.Name = "dev"
	config.Cluster.Network.SetAddresses(my_host)
	config.Logger.Level = loggingLevel
	config.Stats.Enabled = true
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func loadData(path string) ([]Tuple2, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var tuples []Tuple2
	var col0, col1, col2, col3 string
	for {
		if _, err := fmt.Fscanln(f, &col0, &col1, &col2, &col3); err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
		tuples = append(tuples, Tuple2{
			F0: col1,
			F1: col2,
		})
	}
	return tuples, err
}

func main() {
	ctx := context.TODO()

	my_count_64, _ := strconv.ParseInt(os.Getenv("MY_COUNT"), 10, 0)
	my_count := int(my_count_64)
	my_host := os.Getenv("MY_HOST")
	my_input_file := os.Getenv("MY_INPUT_FILE")
	my_map_name_default := os.Getenv("MY_MAP_NAME")
	my_near_cache := os.Getenv("MY_NEAR_CACHE")

	fmt.Printf("--------------------------------------\n")
	fmt.Printf("my_count '%d'\n", my_count)
	fmt.Printf("my_host '%s'\n", my_host)
	fmt.Printf("my_input_file '%s'\n", my_input_file)
	fmt.Printf("my_map_name_default '%s'\n", my_map_name_default)
	fmt.Printf("my_near_cache '%s'\n", my_near_cache)
	fmt.Printf("--------------------------------------\n")

	hazelcastClient := getClient(ctx, my_host)

	startTime := time.Now().Format(time.RFC3339)
	fmt.Printf("~~~~~~\n")
	fmt.Printf("START \n")
	fmt.Printf("~~~~~~\n")
	fmt.Printf("%s ======================================\n", startTime)
	sqlCatalogMapName := "__sql.catalog"
	sqlCatalogMap, _ := hazelcastClient.GetMap(ctx, sqlCatalogMapName)
	sqlCatalogMapSize, _ := sqlCatalogMap.Size(ctx)
	fmt.Printf("Map '%s' has size %d\n", sqlCatalogMapName, sqlCatalogMapSize)
	fmt.Printf("%s ======================================\n", startTime)
	distributedObjectInfo, _ := hazelcastClient.GetDistributedObjectsInfo(ctx)
	mapsCount := len(distributedObjectInfo)
	fmt.Printf("Maps %d\n", mapsCount)
	if mapsCount < 10 {
		for _, distributedObject := range distributedObjectInfo {
			do_name := distributedObject.Name
			do_map, _ := hazelcastClient.GetMap(ctx, do_name)
			do_size, _ := do_map.Size(ctx)
			fmt.Printf("%s %d\n", do_name, do_size)
		}
	}
	fmt.Printf("%s ======================================\n", startTime)

	tuples, err := loadData(my_input_file)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Loaded %d\n", len(tuples))

	wg := &sync.WaitGroup{}
	wg.Add(my_count)
	for i := 0; i < my_count; i++ {
		go worker(i, my_map_name_default, hazelcastClient, tuples, wg)
	}
	wg.Wait()

	endTime := time.Now().Format(time.RFC3339)
	fmt.Printf("~~~~~~\n")
	fmt.Printf("END \n")
	fmt.Printf("~~~~~~\n")
	fmt.Printf("%s ======================================\n", endTime)
	distributedObjectInfo, _ = hazelcastClient.GetDistributedObjectsInfo(ctx)
	mapsCount = len(distributedObjectInfo)
	fmt.Printf("Maps %d\n", mapsCount)
	fmt.Printf("%s ======================================\n", endTime)
	fmt.Printf("Disconnecting\n")
	hazelcastClient.Shutdown(ctx)
}

func worker(id int, my_map_name_default string, client *hazelcast.Client, tuples []Tuple2, wg *sync.WaitGroup) {
	var total int64 = 0
	var worst int64 = math.MinInt64
	var best int64 = math.MaxInt64
	var slow int64 = 0

	ctx := context.Background()
	le := len(tuples)
	for i := (-1 * MAX); i < MAX; i++ {
		e := tuples[rand.Intn(le)]
		var map_name = ""
		var key = ""
		if len(my_map_name_default) == 0 {
			map_name = e.F0
			key = e.F1
		} else {
			if len(my_map_name_default) == 1 {
				end := len(e.F0)
				map_name = e.F0[5:13]
				key = e.F0[14:end]
			} else {
				map_name = my_map_name_default
				key = e.F0 + "-" + e.F1
			}
		}

		tic := time.Now()
		m, _ := client.GetMap(ctx, map_name)
		v, _ := m.Get(ctx, key)
		toc := time.Now()
		elapsed := toc.Sub(tic).Nanoseconds()
		if v == nil {
			fmt.Printf("Nil for %s %s\n", map_name, key)
		}

		// Post Warm-up
		if i >= 0 {
			total = total + elapsed
			if elapsed > worst {
				worst = elapsed
			}
			if elapsed < best {
				best = elapsed
			}
			if elapsed > SLOW_THRESHOLD_NANOS {
				slow++
			}

			if i%100_000 == 0 {
				log_now := time.Now().Format(time.RFC3339)
				fmt.Printf("%d - Worker - count %d (max %d) %s map '%s' key '%s'\n",
					id, i, MAX, log_now, map_name, key)
			}
		}
	}

	avg := total / MAX
	fmt.Printf("%d - Worker - best %d worst %d sum %d avg %d slow %d\n",
		id, best, worst, total, avg, slow)

	wg.Done()
}
