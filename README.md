# nats

## Description

The useful library that makes working with Nats as easy as possible.

## Installation

Run the following command to install the package:

```
go get github.com/minipkg/nats
```

## Basic Usage

### Publisher
```go
import (
    "log"
    "sync"
    "time"
    
    "github.com/labstack/gommon/random"
    "github.com/minipkg/nats"
    "github.com/nats-io/nats.go"
)

const (
    streamName  = "tst"
    subjectName = "test.first"
)

func main() {
    n, err := mp_nats.New(&mp_nats.Config{})
    if err != nil {
        log.Fatal(err)
    }
    
    _, err = n.AddStreamIfNotExists(&nats.StreamConfig{
    Name:     streamName,
    Subjects: []string{"test.>"},
    })
    if err != nil {
        log.Fatalf("natsWriter error: %q", err.Error())
        return
    }
    
    wg := &sync.WaitGroup{}
    for i := 0; i < 10; i++ {
        wg.Add(1)
        tickWriter(wg, n.Js())
    }
    wg.Wait()
}

func tickWriter(wg *sync.WaitGroup, js nats.JetStreamContext) {
    go func() {
        defer wg.Done()
        s := random.String(8)
        t := time.NewTicker(time.Second)
        defer t.Stop()
        
        for {
            select {
            case i := <-t.C:
                js.Publish(subjectName, []byte(s+": hello - "+i.String()))
            }
        }
    }()
}
```

### Push Consumer
```go
import (
	"context"
	"fmt"
	"log"

	"github.com/minipkg/nats"
	"github.com/nats-io/nats.go"
)

const (
	queueGroupName = "groupExample"
	consumerName   = "consumerExample"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	n, err := mp_nats.New(&mp_nats.Config{})
	if err != nil {
		log.Fatal(err)
	}

	_, err = n.AddStreamIfNotExists(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{"test.>"},
	})
	if err != nil {
		log.Fatalf("natsWriter error: %q", err.Error())
	}

	_, _, delFunc, err := n.AddPushConsumerIfNotExists(streamName, &nats.ConsumerConfig{
		Name:    consumerName,
		Durable: consumerName,
		//DeliverGroup:  queueGroupName, // if you want queue group
		FilterSubject: subjectName,
	}, natsMsgHandler)
	if err != nil {
		log.Fatalf("natsWriter error: %q", err.Error())
	}
	defer func() {
		if err = delFunc(); err != nil {
			log.Fatalf("delConsumerAndSubscription error: %q", err.Error())
		}
	}()

	<-ctx.Done()
}

func natsMsgHandler(msg *nats.Msg) {
	fmt.Println(string(msg.Data))
}
```

### Pull Consumer
```go
import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/minipkg/nats"
	"github.com/nats-io/nats.go"
)

const (
	consumerName        = "consumerExample"
	defaultRequestBatch = 1000
	defaultMaxWait      = 3 * time.Second
	duration            = 2 * time.Second
)


func main() {
	ctx, cancel := context.WithCancel(context.Background())

	n, err := mp_nats.New(&mp_nats.Config{})
	if err != nil {
		log.Fatal(err)
	}

	_, err = n.AddStreamIfNotExists(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{"test.>"},
	})
	if err != nil {
		log.Fatalf("natsWriter error: %q", err.Error())
	}

	_, subs, delFunc, err := n.AddPullConsumerIfNotExists(streamName, &nats.ConsumerConfig{
		Name:    consumerName,
		Durable: consumerName,
		FilterSubject: subjectName,
	})
	if err != nil {
		log.Fatalf("natsWriter error: %q", err.Error())
	}
	defer func() {
		if err = delFunc(); err != nil {
			log.Fatalf("delConsumerAndSubscription error: %q", err.Error())
		}
	}()

	listenNatsSubscription(ctx, subs, 0)
	if err != nil {
		log.Fatalf("listenNatsSubscription error: %q", err.Error()))
		return
	}
}

func listenNatsSubscription(ctx context.Context, subs *nats.Subscription, requestBatch uint) error {
	if requestBatch == 0 {
		requestBatch = defaultRequestBatch
	}
OuterLoop:
	for {
		select {
		case <-ctx.Done():
			break OuterLoop
		default:
		}

		bmsgs, err := subs.Fetch(int(requestBatch), nats.MaxWait(defaultMaxWait))
		if err != nil {
			if !errors.Is(err, nats.ErrTimeout) {
				return err
			}

			t := time.NewTimer(duration)
			select {
			case <-ctx.Done():
				break OuterLoop
			case <-t.C:
			}

		}
		for _, msg := range bmsgs {
			if err = msg.Ack(); err != nil {
				return err
			}
			natsMsgHandler(msg)
		}
	}
	return nil
}

func natsMsgHandler(msg *nats.Msg) {
	fmt.Println(string(msg.Data))
}
```