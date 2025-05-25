package consumer

import (
	"fmt"
	"net"
)

// Hostname satisfies the net.Addr interface but as an unresolved name and numeric port.
//
// DNS resolution is thereby pushed down to the dialer that uses this address.
type Hostname struct {
	name string
	port int
}

func (*Hostname) Network() string {
	return "tcp"
}

func (h *Hostname) String() string {
	return net.JoinHostPort(h.name, fmt.Sprintf("%d", h.port))
}
