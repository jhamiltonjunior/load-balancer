package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2"
)

type lbHandler struct {
	gnet.BuiltinEventEngine
	backends []string
	idx      uint32
}

func main() {
	externalHosts := os.Getenv("EXTERNAL_HOSTS")
	if externalHosts == "" {
		log.Fatal("EXTERNAL_HOSTS environment variable not set. Please provide a comma-separated list of unix socket paths.")
	}
	hosts := strings.Split(externalHosts, ",")
	log.Println("Starting gnet/v2 load balancer for hosts:", hosts)

	handler := &lbHandler{backends: hosts}

	if err := gnet.Run(handler, "tcp://:80", gnet.WithMulticore(true)); err != nil {
		panic(err)
	}
}

func (h *lbHandler) OnTraffic(c gnet.Conn) gnet.Action {
	buf, _ := c.Next(-1)
	if len(buf) == 0 {
		return gnet.None
	}

	firstLine, _ := bufio.NewReader(bytes.NewReader(buf)).ReadString('\n')
	parts := strings.Split(strings.TrimSpace(firstLine), " ")
	if len(parts) < 2 {
		c.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
		return gnet.Close
	}
	method := parts[0]
	pathWithQuery := parts[1]
	path := strings.Split(pathWithQuery, "?")[0]

	backend := h.backends[atomic.AddUint32(&h.idx, 1)%uint32(len(h.backends))]

	// Route the request.
	switch {
	case path == "/payments" && method == "POST":
		go forwardUDS(backend, buf, false)
		c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"))
		return gnet.Close

	case path == "/payments-summary" && method == "GET":
		// This is the route that was timing out. The fix is in `forwardUDS`.
		resp := forwardUDS(backend, buf, true)
		if resp != nil {
			c.Write(resp)
		} else {
			c.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		}
		return gnet.Close

	case path == "/purge-payments" && method == "POST":
		go forwardUDS(backend, buf, false)
		c.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
		return gnet.Close
	}

	// No route matched.
	c.Write([]byte("HTTP/1.1 404 Not Found\r\n\r\n"))
	return gnet.Close
}

func forwardUDS(socketPath string, req []byte, waitResp bool) []byte {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		log.Printf("Error connecting to backend '%s': %v", socketPath, err)
		return nil
	}
	defer conn.Close()

	if _, err := conn.Write(req); err != nil {
		log.Printf("Error writing to backend '%s': %v", socketPath, err)
		return nil
	}

	if waitResp {
		b := bufio.NewReader(conn)
		resp, err := http.ReadResponse(b, nil)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				log.Printf("Backend '%s' closed connection before sending full response.", socketPath)
			} else {
				log.Printf("Error reading response from backend '%s': %v", socketPath, err)
			}
			return nil
		}
		defer resp.Body.Close()

		var respBuf bytes.Buffer
		if err := resp.Write(&respBuf); err != nil {
			log.Printf("Error writing response to buffer: %v", err)
			return nil
		}
		return respBuf.Bytes()
	}

	return nil
}
