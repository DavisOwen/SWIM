package main

import (
	"testing"
	"time"
	"sync"
)

func TestTwoNodeCluster(t *testing.T) {
	// Create two nodes
	node1, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}
	defer node1.Shutdown()

	node2, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}
	defer node2.Shutdown()

	// Start both nodes
	node1.Start()
	node2.Start()

	// Node2 joins node1
	err = node2.Join(node1.localAddr)
	if err != nil {
		t.Fatalf("Failed to join: %v", err)
	}

	// Wait for membership convergence
	waitForCondition(t, 5*time.Second, func() bool {
		return len(node1.GetMembers()) == 2 && len(node2.GetMembers()) == 2
	})

	// Verify both nodes know about each other
	node1Members := node1.GetMembers()
	node2Members := node2.GetMembers()

	if len(node1Members) != 2 {
		t.Errorf("Node1 should have 2 members, got %d", len(node1Members))
	}
	if len(node2Members) != 2 {
		t.Errorf("Node2 should have 2 members, got %d", len(node2Members))
	}

	// Check that each node knows about the other
	node1KnowsNode2 := false
	node2KnowsNode1 := false

	for _, member := range node1Members {
		if member.Addr == node2.localAddr {
			node1KnowsNode2 = true
			if member.Status != Alive {
				t.Errorf("Node1 should see node2 as Alive, got %v", member.Status)
			}
		}
	}

	for _, member := range node2Members {
		if member.Addr == node1.localAddr {
			node2KnowsNode1 = true
			if member.Status != Alive {
				t.Errorf("Node2 should see node1 as Alive, got %v", member.Status)
			}
		}
	}

	if !node1KnowsNode2 {
		t.Error("Node1 should know about node2")
	}
	if !node2KnowsNode1 {
		t.Error("Node2 should know about node1")
	}
}

func TestThreeNodeClusterWithFailure(t *testing.T) {
	// Create three nodes
	nodes := make([]*SWIM, 3)
	for i := 0; i < 3; i++ {
		node, err := createTestNode()
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
		defer node.Shutdown()
	}

	// Start all nodes
	for _, node := range nodes {
		node.Start()
	}

	// Form cluster: node1 <- node2 <- node3
	err := nodes[1].Join(nodes[0].localAddr)
	if err != nil {
		t.Fatalf("Failed to join node1: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // Small delay

	err = nodes[2].Join(nodes[1].localAddr)
	if err != nil {
		t.Fatalf("Failed to join node2: %v", err)
	}

	// Wait for full membership convergence
	waitForCondition(t, 10*time.Second, func() bool {
		for _, node := range nodes {
			if len(node.GetMembers()) != 3 {
				return false
			}
		}
		return true
	})

	// Verify all nodes know about each other
	for i, node := range nodes {
		members := node.GetMembers()
		if len(members) != 3 {
			t.Errorf("Node%d should have 3 members, got %d", i, len(members))
		}

		// Check all members are alive
		for _, member := range members {
			if member.Status != Alive {
				t.Errorf("Node%d sees %s as %v, expected Alive", i, member.Addr, member.Status)
			}
		}
	}

	// Simulate node failure by closing connection
	nodes[2].Shutdown()

	// Wait for failure detection
	waitForCondition(t, 15*time.Second, func() bool {
		// Check if remaining nodes detect the failure
		for i := 0; i < 2; i++ {
			members := nodes[i].GetMembers()
			for _, member := range members {
				if member.Addr == nodes[2].localAddr {
					return member.Status == Suspect || member.Status == Dead
				}
			}
		}
		return false
	})

	// Verify failure was detected
	node2Detected := false
	for i := 0; i < 2; i++ {
		members := nodes[i].GetMembers()
		for _, member := range members {
			if member.Addr == nodes[2].localAddr {
				if member.Status == Suspect || member.Status == Dead {
					node2Detected = true
					t.Logf("Node%d detected node2 failure: %v", i, member.Status)
				}
			}
		}
	}

	if !node2Detected {
		t.Error("Failed node should be detected as Suspect or Dead")
	}
}

func TestGossipPropagation(t *testing.T) {
	// Create a chain of 4 nodes
	nodes := make([]*SWIM, 4)
	for i := 0; i < 4; i++ {
		node, err := createTestNode()
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
		defer node.Shutdown()
		node.Start()
	}

	// Form linear chain: node0 -> node1 -> node2 -> node3
	for i := 1; i < 4; i++ {
		err := nodes[i].Join(nodes[i-1].localAddr)
		if err != nil {
			t.Fatalf("Failed to join node%d to node%d: %v", i, i-1, err)
		}
		time.Sleep(200 * time.Millisecond) // Allow gossip to propagate
	}

	// Wait for full membership convergence via gossip
	waitForCondition(t, 20*time.Second, func() bool {
		for i, node := range nodes {
			members := node.GetMembers()
			if len(members) != 4 {
				t.Logf("Node%d has %d members, waiting for 4", i, len(members))
				return false
			}
		}
		return true
	})

	// Verify all nodes discovered each other through gossip
	for i, node := range nodes {
		members := node.GetMembers()
		memberAddrs := make(map[string]bool)
		for _, member := range members {
			memberAddrs[member.Addr] = true
		}

		// Each node should know about all other nodes
		for j, otherNode := range nodes {
			if !memberAddrs[otherNode.localAddr] {
				t.Errorf("Node%d doesn't know about node%d (%s)", i, j, otherNode.localAddr)
			}
		}
	}
}

func TestPingAckMechanism(t *testing.T) {
	node1, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}
	defer node1.Shutdown()

	node2, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}
	defer node2.Shutdown()

	// Start both nodes
	node1.Start()
	node2.Start()

	// Add node2 to node1's member list
	node1.membersMu.Lock()
	node1.members[node2.localAddr] = &Member{
		Addr:        node2.localAddr,
		Status:      Alive,
		Incarnation: 0,
	}
	node1.membersMu.Unlock()

	// Test direct ping
	success := node1.pingWithTimeout(node2.localAddr)
	if !success {
		t.Error("Ping should succeed between healthy nodes")
	}

	// Close node2 and test ping failure
	node2.conn.Close()
	success = node1.pingWithTimeout(node2.localAddr)
	if success {
		t.Error("Ping should fail to closed node")
	}
}

func TestConcurrentOperations(t *testing.T) {
	nodeCount := 5
	nodes := make([]*SWIM, nodeCount)

	// Create nodes
	for i := 0; i < nodeCount; i++ {
		node, err := createTestNode()
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
		defer node.Shutdown()
		node.Start()
	}

	// Join all nodes concurrently to node0
	var wg sync.WaitGroup
	for i := 1; i < nodeCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := nodes[idx].Join(nodes[0].localAddr)
			if err != nil {
				t.Errorf("Failed to join node%d: %v", idx, err)
			}
		}(i)
	}
	wg.Wait()

	// Wait for membership convergence
	waitForCondition(t, 30*time.Second, func() bool {
		for i, node := range nodes {
			members := node.GetMembers()
			if len(members) != nodeCount {
				t.Logf("Node%d has %d members, waiting for %d", i, len(members), nodeCount)
				return false
			}
		}
		return true
	})

	// Verify final state
	for i, node := range nodes {
		members := node.GetMembers()
		if len(members) != nodeCount {
			t.Errorf("Node%d should have %d members, got %d", i, nodeCount, len(members))
		}
	}
}

func BenchmarkPingPerformance(b *testing.B) {
	node1, err := createTestNode()
	if err != nil {
		b.Fatalf("Failed to create node1: %v", err)
	}
	defer node1.Shutdown()

	node2, err := createTestNode()
	if err != nil {
		b.Fatalf("Failed to create node2: %v", err)
	}
	defer node2.Shutdown()

	node1.Start()
	node2.Start()

	// Add node2 to node1's member list
	node1.membersMu.Lock()
	node1.members[node2.localAddr] = &Member{
		Addr:        node2.localAddr,
		Status:      Alive,
		Incarnation: 0,
	}
	node1.membersMu.Unlock()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		node1.pingWithTimeout(node2.localAddr)
	}
}

func TestRapidMembershipChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping rapid membership test in short mode")
	}

	// Create seed node
	seed, err := createTestNode()
	if err != nil {
		t.Fatalf("Failed to create seed: %v", err)
	}
	defer seed.Shutdown()
	seed.Start()

	// Create and join multiple nodes rapidly
	const nodeCount = 10
	nodes := make([]*SWIM, nodeCount)

	for i := 0; i < nodeCount; i++ {
		node, err := createTestNode()
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
		defer node.Shutdown()
		node.Start()

		// Join with small random delay
		time.Sleep(time.Duration(i*10) * time.Millisecond)
		err = node.Join(seed.localAddr)
		if err != nil {
			t.Errorf("Failed to join node%d: %v", i, err)
		}
	}

	// Wait for convergence
	waitForCondition(t, 45*time.Second, func() bool {
		seedMembers := seed.GetMembers()
		return len(seedMembers) >= nodeCount/2 // At least half should be discovered
	})

	t.Logf("Seed node discovered %d out of %d nodes", len(seed.GetMembers()), nodeCount+1)

	// Close half the nodes to test failure detection
	for i := 0; i < nodeCount/2; i++ {
		nodes[i].Shutdown()
	}

	// Wait a bit for failure detection
	time.Sleep(10 * time.Second)

	t.Logf("After failures, seed has %d members", len(seed.GetMembers()))
}
