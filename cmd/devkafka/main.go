// devkafka — Kafka в памяти (kfake) для локального запуска без Docker. Не для продакшена: данные живут, пока жив процесс.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/A160LKN/aqsha/internal/platform"
)

func main() {
	port, _ := strconv.Atoi(platform.Env("PORT", "9092"))
	c, err := kfake.NewCluster(kfake.Ports(port), kfake.SeedTopics(3,
		platform.TopicTransferRequested, platform.TopicTransferResults, platform.TopicTransferDLQ))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Println("Kafka в памяти:", c.ListenAddrs())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
}
