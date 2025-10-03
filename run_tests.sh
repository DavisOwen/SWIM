#!/bin/bash

echo "Running SWIM Protocol Tests"
echo "=========================="

# Run basic unit tests
echo -e "\nRunning Unit Tests..."
go test -v -run "Test(NewSWIM|MemberStatus|MessageSerialization|SelectRandom|GetGossip|UpdateMembership|SWIMConfiguration|GetMembers)" -timeout 30s

# Run integration tests
echo -e "\nRunning Integration Tests..."
go test -v -run "Test(TwoNode|ThreeNode|Gossip|PingAck|Concurrent)" -timeout 60s

# Run rapid membership test separately (longer timeout)
echo -e "\nRunning Rapid Membership Test..."
go test -v -run "TestRapidMembershipChanges" -timeout 120s

# Run benchmarks
echo -e "\nRunning Benchmarks..."
go test -bench=BenchmarkPingPerformance -benchmem -run="^$"

echo -e "\nAll tests completed!"
