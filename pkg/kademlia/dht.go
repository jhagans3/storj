// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package kademlia

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"strconv"

	bkad "github.com/coyle/kademlia"
	"github.com/zeebo/errs"

	proto "storj.io/storj/protos/overlay"
)

// NodeErr is the class for all errors pertaining to node operations
var NodeErr = errs.Class("node error")

//TODO: shouldn't default to TCP but not sure what to do yet
var defaultTransport = proto.NodeTransport_TCP

// Kademlia is an implementation of kademlia adhering to the DHT interface.
type Kademlia struct {
	rt             RoutingTable
	bootstrapNodes []proto.Node
	ip             string
	port           string
	stun           bool
	dht            *bkad.DHT
}

// NewKademlia returns a newly configured Kademlia instance
func NewKademlia(bootstrapNodes []proto.Node, ip string, port string) (*Kademlia, error) {
	bb, err := convertProtoNodes(bootstrapNodes)
	if err != nil {
		return nil, err
	}
	id, err := newID() // TODO() use the real ID type after we settle on an implementation
	if err != nil {
		return nil, err
	}

	bdht, err := bkad.NewDHT(&bkad.MemoryStore{}, &bkad.Options{
		ID:             []byte(id),
		IP:             ip,
		Port:           port,
		BootstrapNodes: bb,
	})

	if err != nil {
		return nil, err
	}

	rt := RouteTable{
		ht:  bdht.HT,
		dht: bdht,
	}

	return &Kademlia{
		rt:             rt,
		bootstrapNodes: bootstrapNodes,
		ip:             ip,
		port:           port,
		stun:           true,
		dht:            bdht,
	}, nil
}

// GetNodes returns all nodes from a starting node up to a maximum limit stored in the local routing table
func (k Kademlia) GetNodes(ctx context.Context, start string, limit int) ([]*proto.Node, error) {
	if start == "" {
		start = k.dht.GetSelfID()
	}

	nn, err := k.dht.FindNodes(ctx, start, limit)
	if err != nil {
		return []*proto.Node{}, err
	}
	return convertNetworkNodes(nn), nil
}

// GetRoutingTable provides the routing table for the Kademlia DHT
func (k *Kademlia) GetRoutingTable(ctx context.Context) (RoutingTable, error) {
	return RouteTable{
		ht:  k.dht.HT,
		dht: k.dht,
	}, nil
}

// Bootstrap contacts one of a set of pre defined trusted nodes on the network and
// begins populating the local Kademlia node
func (k *Kademlia) Bootstrap(ctx context.Context) error {
	return k.dht.Bootstrap()
}

// Ping checks that the provided node is still accessible on the network
func (k *Kademlia) Ping(ctx context.Context, node proto.Node) (proto.Node, error) {
	n, err := convertProtoNode(node)
	if err != nil {
		return proto.Node{}, err
	}

	ok, err := k.dht.Ping(n)
	if err != nil {
		return proto.Node{}, err
	}
	if !ok {
		return proto.Node{}, NodeErr.New("node unavailable")
	}
	return node, nil
}

// FindNode looks up the provided NodeID first in the local Node, and if it is not found
// begins searching the network for the NodeID. Returns and error if node was not found
func (k *Kademlia) FindNode(ctx context.Context, ID NodeID) (proto.Node, error) {
	nodes, err := k.dht.FindNode([]byte(ID))
	if err != nil {
		return proto.Node{}, err

	}

	for _, v := range nodes {
		if string(v.ID) == string(ID) {
			return proto.Node{Id: string(v.ID), Address: &proto.NodeAddress{
				Transport: defaultTransport,
				Address:   fmt.Sprintf("%s:%d", v.IP.String(), v.Port),
			},
			}, nil
		}
	}
	return proto.Node{}, NodeErr.New("node not found")
}

// ListenAndServe connects the kademlia node to the network and listens for incoming requests
func (k *Kademlia) ListenAndServe() error {
	if err := k.dht.CreateSocket(); err != nil {
		return err
	}

	go k.dht.Listen()

	return nil
}

func convertProtoNodes(n []proto.Node) ([]*bkad.NetworkNode, error) {
	nn := make([]*bkad.NetworkNode, len(n))
	for i, v := range n {
		node, err := convertProtoNode(v)
		if err != nil {
			return nil, err
		}
		nn[i] = node
	}

	return nn, nil
}

func convertNetworkNodes(n []*bkad.NetworkNode) []*proto.Node {
	nn := make([]*proto.Node, len(n))
	for i, v := range n {
		nn[i] = convertNetworkNode(v)
	}

	return nn
}

func convertNetworkNode(v *bkad.NetworkNode) *proto.Node {
	return &proto.Node{
		Id:      string(v.ID),
		Address: &proto.NodeAddress{Transport: defaultTransport, Address: net.JoinHostPort(v.IP.String(), strconv.Itoa(v.Port))},
	}
}

func convertProtoNode(v proto.Node) (*bkad.NetworkNode, error) {
	host, port, err := net.SplitHostPort(v.GetAddress().GetAddress())
	if err != nil {
		return nil, err
	}

	nn := bkad.NewNetworkNode(host, port)
	nn.ID = []byte(v.GetId())

	return nn, nil
}

// newID generates a new random ID.
// This purely to get things working. We shouldn't use this as the ID in the actual network
func newID() ([]byte, error) {
	result := make([]byte, 20)
	_, err := rand.Read(result)
	return result, err
}

// GetIntroNode determines the best node to bootstrap a new node onto the network
func GetIntroNode(ip, port string) proto.Node {
	id, _ := newID() // TODO(coyle): This is solely to bootstrap our very first node, after we get an ID, we will just hardcode that ID
	return proto.Node{
		Id: string(id),
		Address: &proto.NodeAddress{
			Transport: defaultTransport,
			Address:   "bootstrap.storj.io:8080",
		},
	}
}
