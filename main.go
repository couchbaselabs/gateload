package main

import (
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/couchbaselabs/gateload/api"
	"github.com/couchbaselabs/gateload/workload"
)

const (
	AUTH_TYPE_SESSION = "session"
	AUTH_TYPE_BASIC   = "basic"
)

func main() {

	log.Printf("gateload main")

	runtime.GOMAXPROCS(runtime.NumCPU())

	// start up an http server, just to serve up expvars
	go http.ListenAndServe(":9876", nil)

	var config workload.Config
	workload.ReadConfig(&config)

	admin := api.SyncGatewayClient{}
	admin.Init(
		config.Hostname,
		config.Database,
		config.Port,
		config.AdminPort,
		config.LogRequests,
	)
	if !admin.Valid() {
		log.Fatalf("unable to connect to sync_gateway, check the hostname and database")
	}

	pendingUsers := make(chan *workload.User)
	users := make([]*workload.User, config.NumPullers+config.NumPushers)

	// start a routine to place pending users into array
	go func() {

		for pendingUser := range pendingUsers {

			log.Printf("got pendingUser from chan.  username: %v, user seq: %v", pendingUser.Name, pendingUser.SeqId)

			// users = append(users, pendingUser)
			users[pendingUser.SeqId-config.UserOffset] = pendingUser
		}
	}()

	rampUpDelay := config.RampUpIntervalMs / (config.NumPullers + config.NumPushers)

	// use a fixed number of workers to create the users/sessions
	userIterator := workload.UserIterator(
		config.NumPullers,
		config.NumPushers,
		config.UserOffset,
		config.ChannelActiveUsers,
		config.ChannelConcurrentUsers,
		config.MinUserOffTimeMs,
		config.MaxUserOffTimeMs,
		rampUpDelay,
		config.RunTimeMs,
	)
	adminWg := sync.WaitGroup{}
	worker := func(workerId int) {
		log.Printf("worker %v executing", workerId)

		defer func() {
			log.Printf("worker %v call adminWg.Done()", workerId)
			adminWg.Done()
			log.Printf("/worker %v call adminWg.Done()", workerId)
		}()

		for user := range userIterator {
			log.Printf("worker %v creating session", workerId)
			createSession(&admin, user, config)
			log.Printf("/worker %v created session", workerId)
			log.Printf("worker %v send to pending users", workerId)
			pendingUsers <- user
			log.Printf("/worker %v sent to pending users", workerId)
		}

	}

	for i := 0; i < 200; i++ {
		adminWg.Add(1)
		log.Printf("kick off go worker %v", i)
		go worker(i)
		log.Printf("/kick off go worker %v", i)
	}

	// wait for all the workers to finish
	log.Printf("wait for all the workers to finish")
	adminWg.Wait()
	log.Printf("/wait for all the workers to finish")

	// close the pending users channel to free that routine
	log.Printf("close the pending users channel to free that routine")
	close(pendingUsers)
	log.Printf("/close the pending users channel to free that routine")

	numChannels := (config.NumPullers + config.NumPushers) / config.ChannelActiveUsers
	channelRampUpDelayMs := time.Duration(config.RampUpIntervalMs/numChannels) * time.Millisecond

	log.Printf("channelRampUpDelayMs: %v. loop over %v users", channelRampUpDelayMs, len(users))

	wg := sync.WaitGroup{}
	channel := ""
	for _, user := range users {
		nextChannel := user.Channel
		if channel != nextChannel {
			if channel != "" {
				log.Printf("Sleeping %v", channelRampUpDelayMs)
				time.Sleep(channelRampUpDelayMs)
				log.Printf("/Sleeping %v", channelRampUpDelayMs)
			}
			channel = nextChannel
		}
		wg := sync.WaitGroup{}

		log.Printf("go runUser with user: %v", user)
		go runUser(user, config, &wg)
		wg.Add(1)
	}

	if config.RunTimeMs > 0 {
		log.Printf("Sleeping %v", time.Duration(config.RunTimeMs-config.RampUpIntervalMs)*time.Millisecond)
		time.Sleep(time.Duration(config.RunTimeMs-config.RampUpIntervalMs) * time.Millisecond)
		log.Printf("Done sleeping.  Shutting down clients")
	} else {
		log.Printf("wg.Wait()")
		wg.Wait()
		log.Printf("/wg.Wait()")
	}
}

func createSession(admin *api.SyncGatewayClient, user *workload.User, config workload.Config) {

	userMeta := api.UserAuth{
		Name:          user.Name,
		Password:      config.Password,
		AdminChannels: []string{user.Channel},
	}
	admin.AddUser(user.Name, userMeta)

	if config.AuthType == AUTH_TYPE_SESSION {

		session := api.Session{Name: user.Name, TTL: 2592000} // 1 month
		log.Printf("====== Creating new session for %s (%s)", user.Type, user.Name)
		user.Cookie = admin.CreateSession(user.Name, session)
		log.Printf("====== Done Creating new session for %s (%s)", user.Type, user.Name)

	}

}

func runUser(user *workload.User, config workload.Config, wg *sync.WaitGroup) {
	log.Printf("Runuser called with %s (%s)", user.Type, user.Name)
	c := api.SyncGatewayClient{}
	c.Init(
		config.Hostname,
		config.Database,
		config.Port,
		config.AdminPort,
		config.LogRequests,
	)
	if config.AuthType == AUTH_TYPE_SESSION {
		c.AddCookie(&user.Cookie)
	} else {
		c.AddUsername(user.Name)
		c.AddPassword(config.Password)
	}

	log.Printf("Starting new %s (%s)", user.Type, user.Name)
	if user.Type == "pusher" {
		go workload.RunNewPusher(
			user.Schedule,
			user.Name,
			&c,
			user.Channel,
			config.DocSize,
			config.SendAttachment,
			config.DocSizeDistribution,
			user.SeqId,
			config.SleepTimeMs,
			wg,
		)
	} else {
		go workload.RunNewPuller(
			user.Schedule,
			&c,
			user.Channel,
			user.Name,
			config.FeedType,
			wg,
		)
	}
	log.Printf("------ Done Starting new %s (%s)", user.Type, user.Name)

}
