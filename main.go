package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type MemberStatus int

const (
	Alive MemberStatus = iota
	Suspect
	Dead
)

func (s MemberStatus) String() string {
	return [...]string{"Alive", "Suspect", "Dead"}[s]
}

type Member struct {
	Addr        string
	Status      MemberStatus
	Incarnation uint64
	SuspectTime time.Time
	mu          sync.RWMutex
}

type MessageType int

const (
	PingMsg MessageType = iota
	AckMsg
	PingReqMsg    // Request other node to ping target
	PingReqAckMsg // Ack response from successful PingReqMsg
	AliveMsg
	SuspectMsg
	DeadMsg
)

type Message struct {
	Type        MessageType
	From        string
	Target      string
	Incarnation uint64
	Members     []MemberInfo
	RequestID   string
	Success     bool
	ForwardFor  string // who we're pinging on behalf of
}

type MemberInfo struct {
	Addr        string
	Status      MemberStatus
	Incarnation uint64
}

type RequestKey struct {
	MsgType   MessageType
	Target    string
	RequestID string
}

var expectedResponse = map[MessageType]MessageType{
	PingMsg:    AckMsg,
	PingReqMsg: PingReqAckMsg,
}

type SWIM struct {
	localAddr   string
	members     map[string]*Member
	membersMu   sync.RWMutex
	conn        *net.UDPConn
	incarnation uint64

	// Configuration
	pendingRequests   map[RequestKey]chan bool
	pendingMu         sync.Mutex
	pingInterval      time.Duration
	pingTimeout       time.Duration
	suspectTimeout    time.Duration
	indirectPingCount int
}

func getPrivateIP() (string, error) {
	// Connect to a remote address (doesn't actually send packets)
	// This tells us which interface/IP we'd use for external connectivity
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

func NewSWIM(port int) (*SWIM, error) {
	ip, err := getPrivateIP()
	if err != nil {
		return nil, fmt.Errorf("failed to detect IP: %v", err)
	}
	localAddr := fmt.Sprintf("%s:%d", ip, port)
	udpAddr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	s := &SWIM{
		localAddr:         localAddr,
		members:           make(map[string]*Member),
		conn:              conn,
		incarnation:       0,
		pendingRequests:   make(map[RequestKey]chan bool),
		pingInterval:      time.Second,
		pingTimeout:       500 * time.Millisecond,
		suspectTimeout:    5 * time.Second,
		indirectPingCount: 3,
	}

	// Add self to members
	s.members[localAddr] = &Member{
		Addr:        localAddr,
		Status:      Alive,
		Incarnation: 0,
	}

	return s, nil
}

// Join adds a seed member to bootstrap the cluster
func (s *SWIM) Join(seedAddr string) error {
	s.membersMu.Lock()
	if _, exists := s.members[seedAddr]; !exists {
		s.members[seedAddr] = &Member{
			Addr:        seedAddr,
			Status:      Alive,
			Incarnation: 0,
		}
	}
	s.membersMu.Unlock()

	// Send ping to seed to establish connection
	return s.sendPing(seedAddr)
}

func (s *SWIM) Start() {
	go s.receiveMessages()
	go s.periodicPing()
	go s.checkSuspects()
}

// Sends ping and waits for response via channels
func (s *SWIM) pingWithTimeout(target string) bool {
	ackChan := make(chan bool, 1)

	pendingPingRequest := RequestKey{MsgType: expectedResponse[PingMsg], Target: target}

	s.pendingMu.Lock()
	s.pendingRequests[pendingPingRequest] = ackChan
	s.pendingMu.Unlock()

	defer func() {
		s.pendingMu.Lock()
		delete(s.pendingRequests, pendingPingRequest)
		s.pendingMu.Unlock()
	}()

	if err := s.sendPing(target); err != nil {
		log.Printf("Failed to send ping to %s: %v", target, err)
		return false
	}

	select {
	case <-ackChan:
		return true
	case <-time.After(s.pingTimeout):
		return false
	}
}

// periodicPing implements the main ping loop
func (s *SWIM) periodicPing() {
	ticker := time.NewTicker(s.pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		target := s.selectRandomMember()
		if target == "" {
			continue
		}

		log.Printf("Pinging %s...", target)

		gotAck := s.pingWithTimeout(target)

		if gotAck {
			log.Printf("%s responded (alive)", target)
			s.markAlive(target)
		} else {
			log.Printf("%s did no respond (timeout)", target)
			gotIndirectAck := s.indirectPing(target)

			if gotIndirectAck {
				log.Printf("%s responded to indirect ping", target)
				s.markAlive(target)
			} else {
				log.Printf("%s failed both direct and indirect ping", target)
				s.markSuspect(target)
			}
		}
	}
}

func (s *SWIM) markAlive(addr string) {
	s.membersMu.Lock()
	defer s.membersMu.Unlock()

	if member, exists := s.members[addr]; exists {
		member.Status = Alive
	}
}

func (s *SWIM) markSuspect(addr string) {
	s.membersMu.Lock()
	defer s.membersMu.Unlock()
	if member, exists := s.members[addr]; exists {
		member.Status = Suspect
		member.SuspectTime = time.Now()
		log.Printf("Marked %s as suspect", addr)
	}
	s.membersMu.Unlock()
}

func (s *SWIM) sendPing(target string) error {
	msg := Message{
		Type:        PingMsg,
		From:        s.localAddr,
		Target:      target,
		Incarnation: s.incarnation,
		Members:     s.getGossipMembers(),
	}
	return s.sendMessage(target, msg)
}

// indirectPing requests other members to ping the target
func (s *SWIM) indirectPing(target string) bool {
	s.membersMu.RLock()
	candidates := make([]string, 0, len(s.members))
	for addr, m := range s.members {
		if addr != s.localAddr && addr != target && m.Status == Alive {
			candidates = append(candidates, addr)
		}
	}
	s.membersMu.RUnlock()

	// Select k random members
	count := s.indirectPingCount
	count = min(len(candidates), count)

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	resultChan := make(chan bool, s.indirectPingCount)

	requestID := fmt.Sprintf("%d", time.Now().UnixNano())

	msgType := PingReqMsg

	requestKey := RequestKey{MsgType: expectedResponse[msgType], Target: target, RequestID: requestID}

	s.pendingMu.Lock()
	s.pendingRequests[requestKey] = resultChan
	s.pendingMu.Unlock()

	defer func() {
		s.pendingMu.Lock()
		delete(s.pendingRequests, requestKey)
		s.pendingMu.Unlock()
	}()

	for i := 0; i < count; i++ {
		msg := Message{
			Type:       msgType,
			From:       s.localAddr,
			Target:     candidates[i],
			ForwardFor: target,
			RequestID:  requestID,
		}
		s.sendMessage(candidates[i], msg)
	}

	select {
	case <-resultChan:
		return true
	case <-time.After(s.pingTimeout):
		return false
	}
}

func (s *SWIM) checkSuspects() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.membersMu.Lock()
		now := time.Now()
		for addr, member := range s.members {
			if member.Status == Suspect {
				if now.Sub(member.SuspectTime) > s.suspectTimeout {
					member.Status = Dead
					log.Printf("Marked %s as dead", addr)
					s.gossipMemberStatus(addr, Dead, member.Incarnation)
				}
			}
		}
		s.membersMu.Unlock()
	}
}

func (s *SWIM) receiveMessages() {
	buf := make([]byte, 4096)
	for {
		n, _, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Error reading UDP: %v", err)
			continue
		}

		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			log.Printf("Error unmarshaling message: %v", err)
			continue
		}

		s.handleMessage(msg)
	}
}

// handleMessage processes received messages
func (s *SWIM) handleMessage(msg Message) {
	// Update membership from gossip
	s.updateMembership(msg.Members)

	switch msg.Type {
	case PingMsg:
		s.handlePing(msg)
	case AckMsg:
		s.handleAck(msg)
	case PingReqMsg:
		s.handlePingReq(msg)
	case PingReqAckMsg:
		s.handlePingReqAckMsg(msg)
	case AliveMsg, SuspectMsg, DeadMsg:
		s.handleStatusUpdate(msg)
	}
}

func (s *SWIM) handlePing(msg Message) {
	ack := Message{
		Type:        AckMsg,
		From:        s.localAddr,
		Incarnation: s.incarnation,
		Members:     s.getGossipMembers(),
	}
	s.sendMessage(msg.From, ack)
}

func (s *SWIM) handleAck(msg Message) {
	s.pendingMu.Lock()
	ackChan, exists := s.pendingRequests[RequestKey{MsgType: msg.Type, Target: msg.From, RequestID: msg.RequestID}]
	s.pendingMu.Unlock()

	if exists {
		select {
		case ackChan <- true:
			log.Printf("Delivered ACK to waiting ping for %s", msg.From)
		default:
		}
	}
}

func (s *SWIM) handlePingReqAckMsg(msg Message) {
	requestKey := RequestKey{
		MsgType:   msg.Type,
		Target:    msg.From,
		RequestID: msg.RequestID,
	}

	s.pendingMu.Lock()
	respChan, exists := s.pendingRequests[requestKey]
	s.pendingMu.Unlock()

	if exists {
		if !msg.Success {
			return
		}
		select {
		case respChan <- true:
		default:
		}
	}
}

// sends ping to ForwardFor on behalf of From
func (s *SWIM) handlePingReq(msg Message) {
	success := s.pingWithTimeout(msg.ForwardFor)

	response := Message{
		Type:       PingReqAckMsg,
		From:       s.localAddr,
		Target:     msg.From,
		ForwardFor: msg.ForwardFor,
		RequestID:  msg.RequestID,
		Success:    success,
	}

	s.sendMessage(msg.From, response)
}

func (s *SWIM) handleStatusUpdate(msg Message) {
	s.membersMu.Lock()
	defer s.membersMu.Unlock()

	if member, exists := s.members[msg.From]; exists {
		// Only update if incarnation is newer
		if msg.Incarnation > member.Incarnation {
			member.Incarnation = msg.Incarnation
			switch msg.Type {
			case SuspectMsg:
				member.Status = Suspect
				member.SuspectTime = time.Now()
			case DeadMsg:
				member.Status = Dead
			case AliveMsg:
				member.Status = Alive
			}
		}
	}
}

// updateMembership updates member list from gossip
func (s *SWIM) updateMembership(members []MemberInfo) {
	s.membersMu.Lock()
	defer s.membersMu.Unlock()

	for _, info := range members {
		if info.Addr == s.localAddr {
			continue
		}

		existing, exists := s.members[info.Addr]
		if !exists {
			s.members[info.Addr] = &Member{
				Addr:        info.Addr,
				Status:      info.Status,
				Incarnation: info.Incarnation,
			}
			log.Printf("Discovered new member: %s", info.Addr)
		} else if info.Incarnation > existing.Incarnation {
			existing.Status = info.Status
			existing.Incarnation = info.Incarnation
			if info.Status == Suspect {
				existing.SuspectTime = time.Now()
			}
		}
	}
}

// selects a random alive member
func (s *SWIM) selectRandomMember() string {
	s.membersMu.RLock()
	defer s.membersMu.RUnlock()

	candidates := make([]string, 0)
	for addr, member := range s.members {
		if addr != s.localAddr && member.Status == Alive {
			candidates = append(candidates, addr)
		}
	}

	if len(candidates) == 0 {
		return ""
	}

	return candidates[rand.Intn(len(candidates))]
}

// returns recent membership changes for gossip
func (s *SWIM) getGossipMembers() []MemberInfo {
	s.membersMu.RLock()
	defer s.membersMu.RUnlock()

	info := make([]MemberInfo, 0, len(s.members))
	for _, member := range s.members {
		info = append(info, MemberInfo{
			Addr:        member.Addr,
			Status:      member.Status,
			Incarnation: member.Incarnation,
		})
	}
	return info
}

// broadcasts status changes
func (s *SWIM) gossipMemberStatus(addr string, status MemberStatus, incarnation uint64) {
	msg := Message{
		Type:        AliveMsg,
		From:        addr,
		Incarnation: incarnation,
	}
	switch status {
	case Suspect:
		msg.Type = SuspectMsg
	case Dead:
		msg.Type = DeadMsg
	}

	// Send to random members
	s.membersMu.RLock()
	for memberAddr := range s.members {
		if memberAddr != s.localAddr && memberAddr != addr {
			s.sendMessage(memberAddr, msg)
		}
	}
	s.membersMu.RUnlock()
}

func (s *SWIM) sendMessage(target string, msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return err
	}

	_, err = s.conn.WriteToUDP(data, addr)
	return err
}

func (s *SWIM) GetMembers() []MemberInfo {
	s.membersMu.RLock()
	defer s.membersMu.RUnlock()

	members := make([]MemberInfo, 0, len(s.members))
	for _, m := range s.members {
		members = append(members, MemberInfo{
			Addr:        m.Addr,
			Status:      m.Status,
			Incarnation: m.Incarnation,
		})
	}
	return members
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: swim <local-port> [seed-addr]")
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(fmt.Sprintf("Invalid port: %v", err))
	}

	swim, err := NewSWIM(port)
	if err != nil {
		log.Fatal(err)
	}

	if len(os.Args) > 2 {
		if err := swim.Join(os.Args[2]); err != nil {
			log.Printf("Failed to join seed: %v", err)
		}
	}

	swim.Start()

	// Print members periodically
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		fmt.Println("\n=== Cluster Members ===")
		for _, m := range swim.GetMembers() {
			fmt.Printf("%s: %s (incarnation: %d)\n", m.Addr, m.Status, m.Incarnation)
		}
	}
}
