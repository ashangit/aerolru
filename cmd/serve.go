package cmd

import (
	aero "github.com/aerospike/aerospike-client-go"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

type ServeCmd struct {
	AerospkeHostname  string `default:"server" help:"available aerospike node from the target cluster"`
	AerospkePort      int    `default:"3000" help:"aerospike port"`
	AerospkeNamespace string `default:"persisted" help:"aerospike namespace"`
	AerospkeSet       string `default:"lru" help:"aerospike set"`
}

func panicOnError(sugar *zap.SugaredLogger, err error) {
	if err != nil {
		sugar.Error(err)
		panic(err)
	}
}

func createNewConnection(clientPolicy *aero.ClientPolicy, host *aero.Host) (*aero.Connection, error) {
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

func (r *ServeCmd) Run() error {
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any
	sugar := logger.Sugar()
	sugar.Info("Start aero lru")

	//asl.Logger.SetLevel(asl.DEBUG)

	// CREATE CLIENT
	client, err := aero.NewClientWithPolicy(nil, r.AerospkeHostname, r.AerospkePort)
	if err != nil {
		sugar.Errorf("Failed to connect to %s:%s due to %s", r.AerospkeHostname, r.AerospkePort, err)
		return err
	}

	// REGISTER UDF
	sugar.Info("Register LRU UDF")
	taskRegister, err := client.RegisterUDFFromFile(nil, "udf/lru.lua", "lru.lua", aero.LUA)
	panicOnError(sugar, err)
	for err := range taskRegister.OnComplete() {
		panicOnError(sugar, err)
	}

	// allow only ONE connection
	clientPolicy := aero.NewClientPolicy()
	clientPolicy.ConnectionQueueSize = 1
	clientPolicy.Timeout = time.Duration(300) * time.Second

	// Const info req
	const sets_req = "sets"
	const histogram_req = "histogram:namespace=persisted;set=lru;type=ttl"

	for {
		// GET LIST OF HOSTS
		nodes := client.GetNodes()
		number_of_hosts := len(nodes)

		var set_ttl map[string]int
		set_ttl = make(map[string]int)

		// Scan for too empty nodes/sets
		for _, node := range nodes {
			host := node.GetHost()
			sugar.Infof("Check node %s", host.Name)

			var infoKeys = []string{sets_req}

			conn, err := createNewConnection(clientPolicy, host)
			rawMetrics, err := aero.RequestInfo(conn, infoKeys...)
			panicOnError(sugar, err)

			// get nb events per sets
			sets := strings.Split(rawMetrics[sets_req], ";")
			for _, set := range sets {
				set_metrics := strings.Split(set, ":")

				if len(set_metrics) < 8 {
					sugar.Debugf("Not enough metrics return for set on %s: %s", host.Name, set_metrics)
					break
				}

				set_name := strings.Split(set_metrics[1], "=")[1]
				set_size, _ := strconv.Atoi(strings.Split(set_metrics[2], "=")[1])

				// Do not check non lru set
				if set_name != "lru" {
					break
				}

				// Const
				const number_of_replica = 2
				soft_limit := 20_000_000 * number_of_replica / number_of_hosts
				hard_limit := 25_000_000 * number_of_replica / number_of_hosts

				if set_size > hard_limit {
					sugar.Infof("Compute ttl to remove for %s set because reach max items: %d/%d", set_name, set_size, hard_limit)

					set_ttl[set_name] = 0

					var infoKeys = []string{histogram_req}

					rawMetrics, err := aero.RequestInfo(conn, infoKeys...)
					panicOnError(sugar, err)

					sugar.Infof("histogram: %s", rawMetrics[histogram_req])

					histogram_metrics := strings.Split(rawMetrics[histogram_req], ":")

					//units := strings.Split(histogram_metrics[0], "=")[1]
					bucket_width, _ := strconv.Atoi(strings.Split(histogram_metrics[2], "=")[1])

					// get buckets
					// TODO: add protection to not remove more than N% of the items N being (hard - soft)/hard * 100 + 5%
					var total_nb_items_removed = 0
					for k, v := range strings.Split(strings.Split(histogram_metrics[3], "=")[1], ",") {
						nb_items, _ := strconv.Atoi(v)
						if nb_items > 0 {
							total_nb_items_removed += nb_items
							if set_size-total_nb_items_removed < soft_limit {
								ttl_to_remove_from_node := (k + 1) * bucket_width
								if ttl_to_remove_from_node > set_ttl[set_name] {
									set_ttl[set_name] = ttl_to_remove_from_node
								}
								break
							}
						}
					}
				}
			}

		}

		//// call udf for each set
		for set_name, ttl := range set_ttl {
			sugar.Infof("Remove item with ttl less than %d on set %s", ttl, set_name)
			qpolicy := aero.NewQueryPolicy()
			stmt := aero.NewStatement(r.AerospkeNamespace, set_name)
			taskExecute, err := client.ExecuteUDF(qpolicy, stmt, "lru", "remove_old_object", aero.NewValue(ttl))
			panicOnError(sugar, err)
			for err := range taskExecute.OnComplete() {
				panicOnError(sugar, err)
			}
		}

		// Sleep before to check again size
		duration_sleep := 20 * time.Minute
		sugar.Infof("Sleep %f min before to restart check set size", duration_sleep.Minutes())
		time.Sleep(duration_sleep)
	}
	return nil
}
