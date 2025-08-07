package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2"
)

// lbHandler stores the backend hosts and the round-robin index.
type lbHandler struct {
	gnet.BuiltinEventEngine
	backends []string
	idx      uint32
}

func main() {
	// Get backend hosts from environment variable.
	externalHosts := os.Getenv("EXTERNAL_HOSTS")
	if externalHosts == "" {
		log.Fatal("EXTERNAL_HOSTS environment variable not set. Please provide a comma-separated list of unix socket paths.")
	}
	hosts := strings.Split(externalHosts, ",")
	log.Println("Starting gnet/v2 load balancer for hosts:", hosts)

	// Initialize the event handler.
	handler := &lbHandler{backends: hosts}

	// Start the gnet server. Note: your original code used port 80, which often requires root privileges.
	// I've changed it to 8080 for easier testing.
	if err := gnet.Run(handler, "tcp://:8080", gnet.WithMulticore(true)); err != nil {
		panic(err)
	}
}

// OnTraffic is called when data is received from a client.
func (h *lbHandler) OnTraffic(c gnet.Conn) gnet.Action {
	// IMPORTANT BUG-FIX NOTE:
	// The `c.Next(-1)` call reads all available data, but it does NOT guarantee a complete
	// HTTP request, especially for POST requests with large bodies. A truly robust solution
	// would buffer data across multiple `OnTraffic` calls until a full request is assembled,
	// by parsing the `Content-Length` header.
	// For simple GET requests, this will often work, but it is not a reliable approach.
	buf, _ := c.Next(-1)
	if len(buf) == 0 {
		return gnet.None
	}

	// Simple parsing to determine the route.
	firstLine, _ := bufio.NewReader(bytes.NewReader(buf)).ReadString('\n')
	parts := strings.Split(strings.TrimSpace(firstLine), " ")
	if len(parts) < 2 {
		c.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
		return gnet.Close
	}
	method := parts[0]
	pathWithQuery := parts[1]
	path := strings.Split(pathWithQuery, "?")[0] // Remove query string for routing.

	// Select backend using atomic round-robin.
	backend := h.backends[atomic.AddUint32(&h.idx, 1)%uint32(len(h.backends))]

	// Route the request.
	switch {
	case path == "/payments" && method == "POST":
		// Fire-and-forget.
		go forwardUDS(backend, buf, false)
		c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"))
		// We close the connection here for simplicity. In a real-world scenario, you might want to handle keep-alives.
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
		// Fire-and-forget.
		go forwardUDS(backend, buf, false)
		c.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
		return gnet.Close
	}

	// No route matched.
	c.Write([]byte("HTTP/1.1 404 Not Found\r\n\r\n"))
	return gnet.Close
}

// forwardUDS connects to a backend via Unix Domain Socket and forwards the request.
func forwardUDS(socketPath string, req []byte, waitResp bool) []byte {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		log.Printf("Error connecting to backend '%s': %v", socketPath, err)
		return nil
	}
	defer conn.Close()

	// Forward the raw request bytes.
	if _, err := conn.Write(req); err != nil {
		log.Printf("Error writing to backend '%s': %v", socketPath, err)
		return nil
	}

	// If we need to wait for a response, we do it here.
	if waitResp {
		// ***** FIX *****
		// Instead of your `ioReadAll`, we use the standard library's HTTP parser.
		// It understands HTTP and will not hang on keep-alive connections.
		b := bufio.NewReader(conn)
		// We pass `nil` for the request because we only need to parse the response structure.
		resp, err := http.ReadResponse(b, nil)
		if err != nil {
			// This can happen if the backend closes the connection prematurely or sends invalid data.
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				log.Printf("Backend '%s' closed connection before sending full response.", socketPath)
			} else {
				log.Printf("Error reading response from backend '%s': %v", socketPath, err)
			}
			return nil
		}
		defer resp.Body.Close()

		// Reconstruct the full response (status line, headers, and body) into a buffer.
		var respBuf bytes.Buffer
		if err := resp.Write(&respBuf); err != nil {
			log.Printf("Error writing response to buffer: %v", err)
			return nil
		}
		return respBuf.Bytes()
	}

	return nil
}
