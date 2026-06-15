package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/dtinth/gojam/pkg/gojam"
)

type ProbeResult struct {
	Server string `json:"server"`
	Quiet  bool   `json:"quiet"`
	Levels []int  `json:"levels"`
	Error  string `json:"error,omitempty"`
}

// gjprobe connects to a Jamulus server as a client, reads one channel level
// update (message ID 1015), prints a JSON result, and disconnects.
// Used to determine whether a non-fleet server is quiet or active.
//
// Output: {"server":"ip:port","quiet":bool,"levels":[...]}
// On timeout: {"server":"ip:port","quiet":true,"levels":[],"error":"timeout"}
func main() {
	server := flag.String("server", "", "server to probe (ip:port)")
	directory := flag.String("directory", "", "directory for UDP hole punching (e.g. anygenre1.jamulus.eu:22224)")
	timeout := flag.Duration("timeout", 5*time.Second, "max wait for level data")
	flag.Parse()

	if *server == "" {
		fmt.Fprintln(os.Stderr, "usage: gjprobe -server ip:port [-directory dir:port] [-timeout 5s]")
		os.Exit(1)
	}

	result := ProbeResult{Server: *server, Levels: []int{}}

	levelCh := make(chan []uint8, 1)

	client, err := gojam.NewClient(*server)
	if err != nil {
		result.Error = err.Error()
		json.NewEncoder(os.Stdout).Encode(result)
		os.Exit(1)
	}
	client.DebugLog = func(string) {}

	client.HandleSoundLevels = func(levels []uint8) {
		select {
		case levelCh <- append([]uint8(nil), levels...):
		default:
		}
	}

	if *directory != "" {
		client.PerformUdpHolePunchingViaDirectory(*directory)
		time.Sleep(400 * time.Millisecond)
	}

	select {
	case levels := <-levelCh:
		result.Levels = make([]int, len(levels))
		quiet := true
		for i, l := range levels {
			result.Levels[i] = int(l)
			if l > 0 {
				quiet = false
			}
		}
		result.Quiet = quiet
	case <-time.After(*timeout):
		result.Error = "timeout"
		result.Quiet = true
	}

	client.Close()
	json.NewEncoder(os.Stdout).Encode(result)
}
