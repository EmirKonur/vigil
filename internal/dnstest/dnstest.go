// Package dnstest runs a tiny authoritative DNS server over UDP. Tests use
// it to exercise DNS checks without the network, and `vigil demo` uses it so
// the demo's DNS monitor has something real to query.
package dnstest

import (
	"net"
	"net/netip"
	"strings"
	"sync"

	"golang.org/x/net/dns/dnsmessage"
)

// Server answers A, AAAA, CNAME, and TXT queries from a static table.
type Server struct {
	conn net.PacketConn

	mu      sync.RWMutex
	records map[string][]string // key: lowercase fqdn + " " + type
	wg      sync.WaitGroup
}

// Listen starts a server on addr ("127.0.0.1:0" for an ephemeral port).
func Listen(addr string) (*Server, error) {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{conn: conn, records: map[string][]string{}}
	s.wg.Go(s.serve)
	return s, nil
}

// Addr is the host:port the server listens on.
func (s *Server) Addr() string { return s.conn.LocalAddr().String() }

// Set replaces the answers for name and type ("A", "AAAA", "CNAME", "TXT").
func (s *Server) Set(name, typ string, values ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[key(name, typ)] = values
}

// Close stops the server.
func (s *Server) Close() error {
	err := s.conn.Close()
	s.wg.Wait()
	return err
}

func key(name, typ string) string {
	return strings.ToLower(strings.TrimSuffix(name, ".")) + " " + strings.ToUpper(typ)
}

func (s *Server) serve() {
	buf := make([]byte, 1500)
	for {
		n, from, err := s.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		if resp, ok := s.answer(buf[:n]); ok {
			_, _ = s.conn.WriteTo(resp, from)
		}
	}
}

func (s *Server) answer(query []byte) ([]byte, bool) {
	var p dnsmessage.Parser
	hdr, err := p.Start(query)
	if err != nil {
		return nil, false
	}
	q, err := p.Question()
	if err != nil {
		return nil, false
	}
	typ := strings.TrimPrefix(q.Type.String(), "Type")

	s.mu.RLock()
	values := s.records[key(q.Name.String(), typ)]
	nameKnown := s.known(q.Name.String())
	s.mu.RUnlock()

	rcode := dnsmessage.RCodeSuccess
	if !nameKnown {
		rcode = dnsmessage.RCodeNameError
	}
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID: hdr.ID, Response: true, Authoritative: true, RecursionDesired: hdr.RecursionDesired, RCode: rcode,
	})
	b.EnableCompression()
	_ = b.StartQuestions()
	_ = b.Question(q)
	_ = b.StartAnswers()
	rh := dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 60}
	for _, v := range values {
		switch q.Type {
		case dnsmessage.TypeA:
			if ip, err := netip.ParseAddr(v); err == nil && ip.Is4() {
				_ = b.AResource(rh, dnsmessage.AResource{A: ip.As4()})
			}
		case dnsmessage.TypeAAAA:
			if ip, err := netip.ParseAddr(v); err == nil && ip.Is6() {
				_ = b.AAAAResource(rh, dnsmessage.AAAAResource{AAAA: ip.As16()})
			}
		case dnsmessage.TypeCNAME:
			if name, err := dnsmessage.NewName(fqdn(v)); err == nil {
				_ = b.CNAMEResource(rh, dnsmessage.CNAMEResource{CNAME: name})
			}
		case dnsmessage.TypeTXT:
			_ = b.TXTResource(rh, dnsmessage.TXTResource{TXT: []string{v}})
		}
	}
	msg, err := b.Finish()
	return msg, err == nil
}

// known reports whether any record exists for name, which decides between
// an empty answer and NXDOMAIN. The caller holds mu.
func (s *Server) known(name string) bool {
	prefix := strings.ToLower(strings.TrimSuffix(name, ".")) + " "
	for k := range s.records {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

func fqdn(name string) string {
	if strings.HasSuffix(name, ".") {
		return name
	}
	return name + "."
}
