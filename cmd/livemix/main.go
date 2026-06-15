package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dtinth/gojam/pkg/jamulusprotocol"
)

const (
	sampleRate   = 48000
	hdrSize      = 44
	frameSize    = 2048 // 42.7ms per tick — 8x fewer disk reads than 256
	mp3Bitrate   = "128k"
	staleFrames  = 24  // ~1s of empty polls before excluding from mix (24 * 42.7ms)
	staleClose   = 240 // ~10s before closing a disconnected track's file handle
)

var pollInterval = time.Duration(int64(time.Second) * frameSize / sampleRate) // ~5.33ms

// Track tails a single per-channel WAV file being written by Jamulus.
type Track struct {
	path     string
	f        *os.File
	numChans int
	mu       sync.Mutex
	buf      []float64 // stereo float samples
	stale    int
}

func openTrack(path string) (*Track, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	hdr := make([]byte, hdrSize)
	if _, err := io.ReadFull(f, hdr); err != nil {
		f.Close()
		return nil, err
	}
	nc := int(binary.LittleEndian.Uint16(hdr[22:24]))
	if nc < 1 || nc > 2 {
		f.Close()
		return nil, fmt.Errorf("unexpected numChannels=%d", nc)
	}
	// Seek to end so we only mix audio written after livemix started.
	// Avoids reading hours of accumulated data into memory on restart.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	log.Printf("track opened: %s (%dch)", filepath.Base(path), nc)
	return &Track{path: path, f: f, numChans: nc}, nil
}

func (t *Track) poll() {
	bps := t.numChans * 2
	raw := make([]byte, frameSize*bps*4)
	n, _ := t.f.Read(raw)
	n = (n / bps) * bps
	if n == 0 {
		t.stale++
		return
	}
	t.stale = 0
	t.mu.Lock()
	for i := 0; i < n/bps; i++ {
		var l, r float64
		if t.numChans == 1 {
			v := float64(int16(binary.LittleEndian.Uint16(raw[i*2:]))) / 32768.0
			l, r = v, v
		} else {
			l = float64(int16(binary.LittleEndian.Uint16(raw[i*4:]))) / 32768.0
			r = float64(int16(binary.LittleEndian.Uint16(raw[i*4+2:]))) / 32768.0
		}
		t.buf = append(t.buf, l, r)
	}
	t.mu.Unlock()
}

// drain extracts exactly n stereo sample pairs, padding with silence.
func (t *Track) drain(n int) []float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]float64, n*2)
	take := n * 2
	if take > len(t.buf) {
		take = len(t.buf)
	}
	copy(out, t.buf[:take])
	t.buf = t.buf[take:]
	return out
}

func (t *Track) close() { t.f.Close() }

// Broadcaster fans out MP3 chunks to all HTTP listeners.
type Broadcaster struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

func newBroadcaster() *Broadcaster {
	return &Broadcaster{clients: make(map[chan []byte]struct{})}
}

func (b *Broadcaster) add(ch chan []byte) {
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
}

func (b *Broadcaster) remove(ch chan []byte) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
}

func (b *Broadcaster) count() int {
	b.mu.RLock()
	n := len(b.clients)
	b.mu.RUnlock()
	return n
}

func (b *Broadcaster) broadcast(data []byte) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients {
		select {
		case ch <- data:
		default:
		}
	}
}

func latestSession(base string) string {
	entries, _ := os.ReadDir(base)
	var dirs []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "Jam-") {
			dirs = append(dirs, e.Name())
		}
	}
	if len(dirs) == 0 {
		return ""
	}
	sort.Strings(dirs)
	return filepath.Join(base, dirs[len(dirs)-1])
}

func wavFiles(dir string) []string {
	entries, _ := os.ReadDir(dir)
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wav") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out
}

func clamp16(v float64) int16 {
	if v > 1.0 {
		return 32767
	}
	if v < -1.0 {
		return -32768
	}
	return int16(v * 32767)
}

// rpcClient holds a persistent JSON-RPC TCP connection.
type rpcClient struct {
	addr   string
	secret string
	conn   net.Conn
	mu     sync.Mutex
}

func newRPCClient(addr, secret string) *rpcClient {
	return &rpcClient{addr: addr, secret: secret}
}

func (r *rpcClient) call(method string, params map[string]interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Connect if not connected
	if r.conn == nil {
		c, err := net.DialTimeout("tcp", r.addr, 3*time.Second)
		if err != nil {
			return fmt.Errorf("rpc connect: %w", err)
		}
		r.conn = c
		// Authenticate
		if err := r.sendMsg(map[string]interface{}{
			"id": 0, "jsonrpc": "2.0",
			"method": "jamulus/apiAuth",
			"params": map[string]interface{}{"secret": r.secret},
		}); err != nil {
			r.conn.Close()
			r.conn = nil
			return err
		}
		buf := make([]byte, 256)
		r.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		r.conn.Read(buf) // consume auth response
		r.conn.SetReadDeadline(time.Time{})
	}

	msg := map[string]interface{}{
		"id": 1, "jsonrpc": "2.0",
		"method": method, "params": params,
	}
	if err := r.sendMsg(msg); err != nil {
		r.conn.Close()
		r.conn = nil
		return err
	}
	buf := make([]byte, 512)
	r.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	r.conn.Read(buf)
	r.conn.SetReadDeadline(time.Time{})
	return nil
}

func (r *rpcClient) sendMsg(v interface{}) error {
	b, _ := json.Marshal(v)
	b = append(b, '\n')
	r.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	_, err := r.conn.Write(b)
	r.conn.SetWriteDeadline(time.Time{})
	return err
}

// jsonSSEClient is the client representation sent in SSE events (matches gojam format).
type jsonSSEClient struct {
	Name       string `json:"name"`
	City       string `json:"city"`
	Country    uint16 `json:"country"`
	Instrument uint32 `json:"instrument"`
	SkillLevel uint8  `json:"skillLevel"`
}

// sseFan fans out SSE event strings to all /events listeners.
type sseFan struct {
	mu      sync.RWMutex
	clients map[chan string]struct{}
}

func newSSEFan() *sseFan { return &sseFan{clients: make(map[chan string]struct{})} }
func (s *sseFan) add(ch chan string) {
	s.mu.Lock()
	s.clients[ch] = struct{}{}
	s.mu.Unlock()
}
func (s *sseFan) remove(ch chan string) {
	s.mu.Lock()
	delete(s.clients, ch)
	s.mu.Unlock()
}
func (s *sseFan) broadcast(data string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for ch := range s.clients {
		select {
		case ch <- data:
		default:
		}
	}
}

// probeUDP1015 sends CLM_REQ_CHANNEL_LEVEL_LIST (1028) to the Jamulus server and
// returns the nibble-unpacked level values. Connectionless — livemix never appears
// as a Jamulus client. Returns nil on timeout or error.
func probeUDP1015(gameAddr string) []int {
	serverUDP, err := net.ResolveUDPAddr("udp", gameAddr)
	if err != nil {
		return nil
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		return nil
	}
	defer conn.Close()

	req := jamulusprotocol.Message{Id: jamulusprotocol.ClmReqChannelLevelList, Counter: 0}
	conn.WriteToUDP(req.Bytes(), serverUDP)

	buf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(time.Second))
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			return nil
		}
		msg, err := jamulusprotocol.ParseMessage(buf[:n])
		if err != nil {
			continue
		}
		if msg.Id == jamulusprotocol.ClmChannelLevelList {
			levels := make([]int, len(msg.Data)*2)
			for i, b := range msg.Data {
				levels[i*2] = int(b & 0x0f)
				levels[i*2+1] = int(b >> 4)
			}
			return levels
		}
	}
}

func main() {
	baseDir := flag.String("dir", "", "recording base directory (required)")
	addr := flag.String("addr", ":8765", "HTTP listen address")
	rpcAddr := flag.String("rpc", "localhost:9999", "Jamulus JSON-RPC address")
	rpcSecret := flag.String("secret", "lounge-rpc-key-ok", "JSON-RPC secret")
	streamURL := flag.String("url", "https://ear.jamulus.live", "public stream URL for chat announcements")
	gameAddr := flag.String("game", "localhost:22224", "Jamulus UDP game port for level probing")
	flag.Parse()

	if *baseDir == "" {
		log.Fatal("-dir is required")
	}

	bc := newBroadcaster()
	rpc := newRPCClient(*rpcAddr, *rpcSecret)

	sse := newSSEFan()
	var sseMu sync.Mutex
	sseClients := `{"clients":[]}`
	sseLevels := `{"levels":[]}`

	// Start ffmpeg
	ffCmd := exec.Command("ffmpeg",
		"-loglevel", "error",
		"-f", "s16le", "-ar", "48000", "-ac", "2",
		"-i", "pipe:0",
		"-f", "mp3", "-b:a", mp3Bitrate, "pipe:1",
	)
	ffIn, err := ffCmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}
	ffOut, err := ffCmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := ffCmd.Start(); err != nil {
		log.Fatal("ffmpeg:", err)
	}
	log.Printf("ffmpeg pid %d", ffCmd.Process.Pid)

	// Forward ffmpeg output to broadcaster
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := ffOut.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				bc.broadcast(chunk)
			}
			if err != nil {
				log.Printf("ffmpeg stdout closed: %v", err)
				return
			}
		}
	}()

	// Mixing loop
	go func() {
		var tracks []*Track
		curSession := ""
		knownChannels := map[int]bool{}
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		// Poll getClients every 5s for new joiners
		clientTicker := time.NewTicker(5 * time.Second)
		defer clientTicker.Stop()

		for {
			select {
			case <-clientTicker.C:
				if curSession == "" {
					knownChannels = map[int]bool{}
					continue
				}
				// Poll getClients: send welcome to new joiners + update SSE state
				go func() {
					c, err := net.DialTimeout("tcp", *rpcAddr, 3*time.Second)
					if err != nil {
						return
					}
					defer c.Close()
					send := func(v interface{}) {
						b, _ := json.Marshal(v)
						c.Write(append(b, '\n'))
					}
					recv := func() map[string]interface{} {
						buf := make([]byte, 4096)
						c.SetReadDeadline(time.Now().Add(3 * time.Second))
						n, _ := c.Read(buf)
						var out map[string]interface{}
						json.Unmarshal(buf[:n], &out)
						return out
					}
					send(map[string]interface{}{"id": 0, "jsonrpc": "2.0", "method": "jamulus/apiAuth", "params": map[string]interface{}{"secret": *rpcSecret}})
					recv()
					send(map[string]interface{}{"id": 1, "jsonrpc": "2.0", "method": "jamulusserver/getClients", "params": map[string]interface{}{}})
					resp := recv()
					result, ok := resp["result"].(map[string]interface{})
					if !ok {
						return
					}
					clientsRaw, _ := result["clients"].([]interface{})

					// Build a slot-indexed map so clients[i] aligns with levels[i]
					// from the 1028 nibble array (both indexed by channel slot).
					slotMap := map[int]jsonSSEClient{}
					maxSlot := -1
					for _, ci := range clientsRaw {
						cm, _ := ci.(map[string]interface{})
						idF, _ := cm["id"].(float64)
						id := int(idF)
						name, _ := cm["name"].(string)
						city, _ := cm["city"].(string)
						instrumentF, _ := cm["instrumentCode"].(float64)
						skillF, _ := cm["skillLevelCode"].(float64)
						slotMap[id] = jsonSSEClient{
							Name: name, City: city,
							Instrument: uint32(instrumentF), SkillLevel: uint8(skillF),
						}
						if id > maxSlot {
							maxSlot = id
						}
						if !knownChannels[id] {
							knownChannels[id] = true
							welcomeMsg := fmt.Sprintf("🔴 Stream live at <a href=\"%s\">%s</a>", *streamURL, *streamURL)
							send(map[string]interface{}{"id": 2, "jsonrpc": "2.0", "method": "jamulusserver/sendClientChatMessage", "params": map[string]interface{}{"channelId": id, "message": welcomeMsg}})
							recv()
						}
					}

					// Probe UDP 1028 for levels (connectionless — does not appear as a client)
					levels := probeUDP1015(*gameAddr)
					if levels == nil {
						levels = []int{}
					}

					// Build dense clients list sorted by slot, with a matching
					// levels array so clients[i] aligns with levels[i].
					type slotClient struct {
						slot int
						c    jsonSSEClient
					}
					var ordered []slotClient
					for slot, c := range slotMap {
						ordered = append(ordered, slotClient{slot, c})
					}
					// sort by slot number
					for i := 1; i < len(ordered); i++ {
						for j := i; j > 0 && ordered[j].slot < ordered[j-1].slot; j-- {
							ordered[j], ordered[j-1] = ordered[j-1], ordered[j]
						}
					}
					sseClientList := make([]jsonSSEClient, len(ordered))
					alignedLevels := make([]int, len(ordered))
					for i, sc := range ordered {
						sseClientList[i] = sc.c
						if sc.slot < len(levels) {
							alignedLevels[i] = levels[sc.slot]
						}
					}

					// Update shared SSE state and broadcast to listeners
					clientsJSON, _ := json.Marshal(map[string]interface{}{"clients": sseClientList})
					levelsJSON, _ := json.Marshal(map[string]interface{}{"levels": alignedLevels})
					sseMu.Lock()
					sseClients = string(clientsJSON)
					sseLevels = string(levelsJSON)
					sseMu.Unlock()
					sse.broadcast(sseClients)
					sse.broadcast(sseLevels)
				}()

			case <-ticker.C:
				// Session change detection
				session := latestSession(*baseDir)
				if session != curSession {
					prev := curSession
					for _, t := range tracks {
						t.close()
					}
					tracks = nil
					knownChannels = map[int]bool{}
					curSession = session

					if prev != "" {
						// Session ended
						log.Printf("session ended")
						go rpc.call("jamulusserver/setRecordingBanner", map[string]interface{}{"active": false})
					}
					if session != "" {
						log.Printf("session started: %s", filepath.Base(session))
						go rpc.call("jamulusserver/setRecordingBanner", map[string]interface{}{"active": true})
						msg := fmt.Sprintf("🔴 Streaming live at <a href=\"%s\">%s</a>", *streamURL, *streamURL)
						go rpc.call("jamulusserver/broadcastChat", map[string]interface{}{"message": msg})
					}
				}

				// Detect new WAV files
				if session != "" {
					known := make(map[string]bool, len(tracks))
					for _, t := range tracks {
						known[t.path] = true
					}
					for _, f := range wavFiles(session) {
						if !known[f] {
							t, err := openTrack(f)
							if err != nil {
								// EOF means file too small yet (header still being written); retry next tick silently
								if err != io.EOF && !strings.HasSuffix(err.Error(), "EOF") {
									log.Printf("openTrack %s: %v", filepath.Base(f), err)
								}
								continue
							}
							tracks = append(tracks, t)
						}
					}
				}

				// Poll all tracks; close and drop any that have been stale too long.
				for _, t := range tracks {
					t.poll()
				}
				var live []*Track
				for _, t := range tracks {
					if t.stale > staleClose {
						log.Printf("track closed (stale): %s", filepath.Base(t.path))
						t.close()
					} else {
						live = append(live, t)
					}
				}
				tracks = live

				// Mix frameSize stereo samples
				mixed := make([]float64, frameSize*2)
				for _, t := range tracks {
					if t.stale > staleFrames {
						continue
					}
					samples := t.drain(frameSize)
					for i, v := range samples {
						mixed[i] += v
					}
				}

				// Only encode when someone is listening
				if bc.count() == 0 {
					continue
				}

				out := make([]byte, frameSize*4)
				for i := 0; i < frameSize*2; i++ {
					s := clamp16(mixed[i])
					binary.LittleEndian.PutUint16(out[i*2:], uint16(s))
				}
				if _, err := ffIn.Write(out); err != nil {
					log.Printf("ffmpeg write: %v", err)
				}
			}
		}
	}()

	// HTTP endpoints
	http.HandleFunc("/stream.mp3", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("listener connected: %s", r.RemoteAddr)
		w.Header().Set("Content-Type", "audio/mpeg")
		w.Header().Set("Cache-Control", "no-cache")
		ch := make(chan []byte, 64)
		bc.add(ch)
		defer func() {
			bc.remove(ch)
			log.Printf("listener disconnected: %s", r.RemoteAddr)
		}()
		for {
			select {
			case data := <-ch:
				if _, err := w.Write(data); err != nil {
					return
				}
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			case <-r.Context().Done():
				return
			}
		}
	})

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		ch := make(chan string, 16)
		sse.add(ch)
		defer sse.remove(ch)
		sseMu.Lock()
		c, l := sseClients, sseLevels
		sseMu.Unlock()
		fmt.Fprintf(w, "data: %s\n\ndata: %s\n\n", c, l)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		for {
			select {
			case data := <-ch:
				fmt.Fprintf(w, "data: %s\n\n", data)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			case <-r.Context().Done():
				return
			}
		}
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		session := latestSession(*baseDir)
		if session == "" {
			session = "(none)"
		}
		fmt.Fprintf(w, "livemix\nsession: %s\n", filepath.Base(session))
	})

	log.Printf("livemix listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
