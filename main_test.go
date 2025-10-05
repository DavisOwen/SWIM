package main

import (
	"testing"
	"time"
	"fmt"
	"net"
)

func findAvailablePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port
}

func createTestNode() (*SWIM, error) {
	port := findAvailablePort()
	return NewSWIM(port)
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	start := time.Now()
	for time.Since(start) < timeout {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Condition not met within timeout")
}

func TestNewSWIM(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create SWIM node: %v", err)
	}
	defer node.Shutdown()

	if node.localAddr == "" {
		t.Error("Local address should not be empty")
	}

	if len(node.members) != 1 {
		t.Errorf("Expected 1 member (self), got %d", len(node.members))
	}

	self := node.members[node.localAddr]
	if self == nil {
		t.Error("Self should be in members")
	}
	if self.Status != Alive {
		t.Errorf("Self status should be Alive, got %v", self.Status)
	}
}

func TestMemberStatusTransitions(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create SWIM node: %v", err)
	}
	defer node.Shutdown()

	// Add a test member
	testAddr := "127.0.0.1:9999"
	node.members[testAddr] = &Member{
		Addr:        testAddr,
		Status:      Alive,
		Incarnation: 0,
	}

	// Test marking as suspect
	node.markSuspect(testAddr)
	member := node.members[testAddr]
	if member.Status != Suspect {
		t.Errorf("Expected member status to be Suspect, got %v", member.Status)
	}
	if member.SuspectTime.IsZero() {
		t.Error("SuspectTime should be set")
	}

	// Test marking as alive
	node.markAlive(testAddr)
	if member.Status != Alive {
		t.Errorf("Expected member status to be Alive, got %v", member.Status)
	}
}

func TestMessageSerialization(t *testing.T) {
	msg := Message{
		Type:        PingMsg,
		From:        "127.0.0.1:8000",
		Target:      "127.0.0.1:8001",
		Incarnation: 1,
		Members: []MemberInfo{
			{Addr: "127.0.0.1:8002", Status: Alive, Incarnation: 2},
		},
	}

	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create SWIM node: %v", err)
	}
	defer node.Shutdown()

	// This should not panic
	err = node.sendMessage("127.0.0.1:9999", msg)
	// We expect an error since the target doesn't exist, but no panic
	if err == nil {
		t.Log("Message sent successfully (or target didn't respond)")
	}
}

func TestSelectRandomMember(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create SWIM node: %v", err)
	}
	defer node.Shutdown()

	// With only self, should return empty
	target := node.selectRandomMember()
	if target != "" {
		t.Errorf("Expected empty target with only self, got %s", target)
	}

	// Add some members
	for i := 0; i < 3; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 9000+i)
		node.members[addr] = &Member{
			Addr:        addr,
			Status:      Alive,
			Incarnation: 0,
		}
	}

	// Should return one of the added members
	target = node.selectRandomMember()
	if target == "" {
		t.Error("Expected non-empty target")
	}
	if target == node.localAddr {
		t.Error("Should not select self")
	}

	// Add a dead member
	deadAddr := "127.0.0.1:9999"
	node.members[deadAddr] = &Member{
		Addr:        deadAddr,
		Status:      Dead,
		Incarnation: 0,
	}

	// Should not select dead members
	for i := 0; i < 10; i++ {
		target = node.selectRandomMember()
		if target == deadAddr {
			t.Error("Should not select dead member")
		}
	}
}

func TestGetGossipMembers(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create SWIM node: %v", err)
	}
	defer node.Shutdown()

	// Add some members
	for i := 0; i < 3; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 9000+i)
		node.members[addr] = &Member{
			Addr:        addr,
			Status:      Alive,
			Incarnation: uint64(i),
		}
	}

	gossip := node.getGossipMembers()
	if len(gossip) != 4 { // 3 added + self
		t.Errorf("Expected 4 gossip members, got %d", len(gossip))
	}

	// Check that gossip includes all members
	found := make(map[string]bool)
	for _, info := range gossip {
		found[info.Addr] = true
	}

	if !found[node.localAddr] {
		t.Error("Gossip should include self")
	}
}

func TestUpdateMembership(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create SWIM node: %v", err)
	}
	defer node.Shutdown()

	updates := []MemberInfo{
		{Addr: "127.0.0.1:9000", Status: Alive, Incarnation: 1},
		{Addr: "127.0.0.1:9001", Status: Suspect, Incarnation: 2},
	}

	node.updateMembership(updates)

	if len(node.members) != 3 { // 2 added + self
		t.Errorf("Expected 3 members, got %d", len(node.members))
	}

	// Check first member
	member1 := node.members["127.0.0.1:9000"]
	if member1 == nil {
		t.Error("First member should exist")
	} else {
		if member1.Status != Alive {
			t.Errorf("Expected Alive status, got %v", member1.Status)
		}
		if member1.Incarnation != 1 {
			t.Errorf("Expected incarnation 1, got %d", member1.Incarnation)
		}
	}

	// Test incarnation update
	newUpdates := []MemberInfo{
		{Addr: "127.0.0.1:9000", Status: Suspect, Incarnation: 3},
	}
	node.updateMembership(newUpdates)

	member1 = node.members["127.0.0.1:9000"]
	if member1.Status != Suspect {
		t.Errorf("Expected Suspect status after update, got %v", member1.Status)
	}
	if member1.Incarnation != 3 {
		t.Errorf("Expected incarnation 3, got %d", member1.Incarnation)
	}

	// Test old incarnation should be ignored
	oldUpdates := []MemberInfo{
		{Addr: "127.0.0.1:9000", Status: Alive, Incarnation: 2},
	}
	node.updateMembership(oldUpdates)

	member1 = node.members["127.0.0.1:9000"]
	if member1.Status != Suspect {
		t.Error("Status should remain Suspect for old incarnation")
	}
}

func TestSWIMConfiguration(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create SWIM node: %v", err)
	}
	defer node.Shutdown()

	// Check default values
	if node.pingInterval != time.Second {
		t.Errorf("Expected ping interval 1s, got %v", node.pingInterval)
	}
	if node.pingTimeout != 500*time.Millisecond {
		t.Errorf("Expected ping timeout 500ms, got %v", node.pingTimeout)
	}
	if node.suspectTimeout != 5*time.Second {
		t.Errorf("Expected suspect timeout 5s, got %v", node.suspectTimeout)
	}
	if node.indirectPingCount != 3 {
		t.Errorf("Expected indirect ping count 3, got %d", node.indirectPingCount)
	}
}

func TestGetMembers(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create SWIM node: %v", err)
	}
	defer node.Shutdown()

	// Add some test members
	node.members["127.0.0.1:9000"] = &Member{
		Addr:        "127.0.0.1:9000",
		Status:      Alive,
		Incarnation: 1,
	}
	node.members["127.0.0.1:9001"] = &Member{
		Addr:        "127.0.0.1:9001",
		Status:      Suspect,
		Incarnation: 2,
	}

	members := node.GetMembers()
	if len(members) != 3 { // 2 added + self
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	// Verify members contain correct information
	memberMap := make(map[string]MemberInfo)
	for _, m := range members {
		memberMap[m.Addr] = m
	}

	if info, exists := memberMap["127.0.0.1:9000"]; exists {
		if info.Status != Alive {
			t.Errorf("Expected Alive status, got %v", info.Status)
		}
		if info.Incarnation != 1 {
			t.Errorf("Expected incarnation 1, got %d", info.Incarnation)
		}
	} else {
		t.Error("Member 127.0.0.1:9000 not found")
	}
}
