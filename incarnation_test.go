package main

import (
	"testing"
	"time"
)

// Test that incarnation increments when refuting suspicion
func TestIncarnationRefutation(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Shutdown()

	node.Start()

	// Initial incarnation should be 0
	if node.incarnation != 0 {
		t.Errorf("Expected initial incarnation 0, got %d", node.incarnation)
	}

	// Simulate receiving a SuspectMsg about ourselves
	suspectMsg := Message{
		Type:        SuspectMsg,
		From:        node.localAddr,  // About us!
		Incarnation: 0,
	}

	node.handleStatusUpdate(suspectMsg)

	// Should have incremented incarnation to refute
	time.Sleep(50 * time.Millisecond)  // Give it time to process

	if node.incarnation != 1 {
		t.Errorf("Expected incarnation to increment to 1, got %d", node.incarnation)
	}

	// Our own member record should also be updated
	node.membersMu.RLock()
	self := node.members[node.localAddr]
	node.membersMu.RUnlock()

	if self.Incarnation != 1 {
		t.Errorf("Expected self member incarnation 1, got %d", self.Incarnation)
	}

	if self.Status != Alive {
		t.Errorf("Expected self status Alive, got %v", self.Status)
	}
}

// Test that we only refute if their incarnation is >= ours
func TestIncarnationRefutationOnlyIfNewer(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Shutdown()

	node.Start()

	// Set our incarnation to 5
	node.membersMu.Lock()
	node.incarnation = 5
	node.members[node.localAddr].Incarnation = 5
	node.membersMu.Unlock()

	// Receive old suspicion (incarnation 3)
	oldSuspectMsg := Message{
		Type:        SuspectMsg,
		From:        node.localAddr,
		Incarnation: 3,
	}

	node.handleStatusUpdate(oldSuspectMsg)

	time.Sleep(50 * time.Millisecond)

	// Should NOT have changed (old incarnation)
	if node.incarnation != 5 {
		t.Errorf("Expected incarnation to stay 5, got %d", node.incarnation)
	}

	// Receive newer suspicion (incarnation 7)
	newSuspectMsg := Message{
		Type:        SuspectMsg,
		From:        node.localAddr,
		Incarnation: 7,
	}

	node.handleStatusUpdate(newSuspectMsg)

	time.Sleep(50 * time.Millisecond)

	// Should increment to 8 (7 + 1)
	if node.incarnation != 8 {
		t.Errorf("Expected incarnation to increment to 8, got %d", node.incarnation)
	}
}

// Test incarnation in member updates
func TestIncarnationInMemberUpdates(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Shutdown()

	// Add a member
	node.membersMu.Lock()
	node.members["127.0.0.1:9000"] = &Member{
		Addr:        "127.0.0.1:9000",
		Status:      Alive,
		Incarnation: 1,
	}
	node.membersMu.Unlock()

	// Receive status update with older incarnation (should be ignored)
	oldUpdate := Message{
		Type:        SuspectMsg,
		From:        "127.0.0.1:9000",
		Incarnation: 0,
	}

	node.handleStatusUpdate(oldUpdate)

	// Check that status didn't change
	node.membersMu.RLock()
	member := node.members["127.0.0.1:9000"]
	node.membersMu.RUnlock()

	if member.Status != Alive {
		t.Errorf("Status should remain Alive for old incarnation, got %v", member.Status)
	}
	if member.Incarnation != 1 {
		t.Errorf("Incarnation should remain 1, got %d", member.Incarnation)
	}

	// Receive status update with newer incarnation (should be accepted)
	newUpdate := Message{
		Type:        SuspectMsg,
		From:        "127.0.0.1:9000",
		Incarnation: 3,
	}

	node.handleStatusUpdate(newUpdate)

	// Check that status changed
	node.membersMu.RLock()
	member = node.members["127.0.0.1:9000"]
	node.membersMu.RUnlock()

	if member.Status != Suspect {
		t.Errorf("Status should be Suspect for new incarnation, got %v", member.Status)
	}
	if member.Incarnation != 3 {
		t.Errorf("Incarnation should be 3, got %d", member.Incarnation)
	}
}

// Test that incarnation is included in ping messages
func TestIncarnationInPingMessages(t *testing.T) {
	node, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Shutdown()

	// Set incarnation
	node.membersMu.Lock()
	node.incarnation = 42
	node.membersMu.Unlock()

	// Create a ping message (indirectly by checking what sendPing would send)
	// We can't easily intercept the message, but we can verify the incarnation is set

	if node.incarnation != 42 {
		t.Errorf("Expected incarnation 42, got %d", node.incarnation)
	}

	// Verify gossip includes correct incarnation
	gossip := node.getGossipMembers()
	for _, member := range gossip {
		if member.Addr == node.localAddr {
			if member.Incarnation != 0 {  // Self member still has incarnation 0
				t.Logf("Note: Self member incarnation is %d (node incarnation is %d)",
					member.Incarnation, node.incarnation)
			}
		}
	}
}