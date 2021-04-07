package cmd

import (
	aero "github.com/aerospike/aerospike-client-go"
	asl "github.com/aerospike/aerospike-client-go/logger"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

type ServeCmd struct {
	AerospkeHostname  string `default:"48-df-37-7e-51-20.storage.criteo.preprod" help:"available aerospike node from the target cluster"`
	AerospkePort      int    `default:"3000" help:"aerospike port"`
	AerospkeNamespace string `default:"persisted" help:"aerospike namespace"`
	AerospkeSet       string `default:"lru" help:"aerospike set"`
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// TODO: discover list of nodes
// query all nodes

func (r *ServeCmd) Run() error {
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any
	sugar := logger.Sugar()
	sugar.Info("Start aero lru")

	asl.Logger.SetLevel(asl.DEBUG)
	clientPolicy := aero.NewClientPolicy()

	// allow only ONE connection
	clientPolicy.ConnectionQueueSize = 1
	clientPolicy.Timeout = time.Duration(300) * time.Second
	client, err := aero.NewClientWithPolicy(nil, r.AerospkeHostname, r.AerospkePort)
	if err != nil {
		sugar.Errorf("Failed to connect to %s:%s due to %s", r.AerospkeHostname, r.AerospkePort, err)
		return err
	}
	task, err := client.RegisterUDFFromFile(nil, "udf/lru.lua", "lru.lua", aero.LUA)
	if err != nil {
		panicOnError(err)
	}
	for err := range task.OnComplete() {
		if err != nil {
			panicOnError(err)
		}
	}

	host := aero.NewHost(r.AerospkeHostname, int(r.AerospkePort))

	createNewConnection := func() (*aero.Connection, error) {
		conn, err := aero.NewConnection(clientPolicy, host)
		if err != nil {
			return nil, err
		}

		if clientPolicy.RequiresAuthentication() {
			if err := conn.Login(clientPolicy); err != nil {
				return nil, err
			}
		}

		// Set no connection deadline to re-use connection, but socketTimeout will be in effect
		var deadline time.Time
		err = conn.SetTimeout(deadline, clientPolicy.Timeout)
		if err != nil {
			return nil, err
		}

		return conn, nil
	}

	// Info request
	const sets_req = "sets"

	var infoKeys = []string{sets_req}

	conn, err := createNewConnection()
	rawMetrics, err := aero.RequestInfo(conn, infoKeys...)
	if err != nil {
		sugar.Error(err)
		panicOnError(err)
	}

	sugar.Infof("sets: %s", rawMetrics[sets_req])

	// get nb events
	sets := strings.Split(rawMetrics[sets_req], ";")
	for _, set := range sets {
		set_metrics := strings.Split(set, ":")
		var object_number = 0
		for _, set_metric := range set_metrics {
			//println(set_metric)
			if strings.Contains(set_metric, "objects=") {
				object_number, _ = strconv.Atoi(strings.Split(set_metric, "=")[1])
				break
			}
		}

		// do clean up if limit raise
		// TODO get it from sum of all nodes
		if object_number > 10000 {
			sugar.Info("Reduce size of lru set because reach max items")

			const histogram_req = "histogram:namespace=persisted;set=lru;type=ttl"

			var infoKeys = []string{histogram_req}

			conn, err := createNewConnection()
			rawMetrics, err := aero.RequestInfo(conn, infoKeys...)
			if err != nil {
				sugar.Error(err)
				panicOnError(err)
			}
			sugar.Infof("histogram: %s", rawMetrics[histogram_req])
			histogram_metrics := strings.Split(rawMetrics[histogram_req], ":")
			units := strings.Split(histogram_metrics[0], "=")[1]
			println(units)
			bucket_width, _ := strconv.Atoi(strings.Split(histogram_metrics[2], "=")[1])
			for _, histogram_metric := range histogram_metrics {
				// get buckets
				if strings.Contains(histogram_metric, "buckets=") {
					var ttl_to_remove_from = 0
					//println(histogram_metric)
					for k, v := range strings.Split(histogram_metric, ",") {
						println(k)
						nb_items, _ := strconv.Atoi(v)
						if nb_items > 0 {
							println(k)
							println(nb_items)
							ttl_to_remove_from = (k + 1) * bucket_width
							println((k + 1) * bucket_width)
							break
						}
					}
					// select ttl to remove
					println(ttl_to_remove_from)

					// call udf
					qpolicy := aero.NewQueryPolicy()
					stmt := aero.NewStatement(r.AerospkeNamespace, r.AerospkeSet)
					task, err := client.ExecuteUDF(qpolicy, stmt, "lru", "remove_old_object", aero.NewValue(ttl_to_remove_from))
					if err != nil {
						panicOnError(err)
					}
					for err := range task.OnComplete() {
						if err != nil {
							panicOnError(err)
						}
					}
					break
				}
			}

		}
	}
	return nil
}
