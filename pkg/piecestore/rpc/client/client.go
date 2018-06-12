// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package client

import (
	"fmt"
	"io"
	"log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	monkit "gopkg.in/spacemonkeygo/monkit.v2"

	pb "storj.io/storj/protos/piecestore"
)

var (
	mon = monkit.Package()
)

// Client -- Struct Info needed for protobuf api calls
type Client struct {
	ctx   context.Context
	route pb.PieceStoreRoutesClient
}

// New -- Initilize Client
func New(ctx context.Context, conn *grpc.ClientConn) *Client {
	return &Client{ctx: ctx, route: pb.NewPieceStoreRoutesClient(conn)}
}

// NewCustomRoute creates new Client with custom route interface
func NewCustomRoute(ctx context.Context, route pb.PieceStoreRoutesClient) *Client {
	return &Client{ctx: ctx, route: route}
}

// PieceMetaRequest -- Request info about a piece by Id
func (client *Client) PieceMetaRequest(id string) (rv *pb.PieceSummary, err error) {
	defer mon.Task()(nil)(&err)
	return client.route.Piece(client.ctx, &pb.PieceId{Id: id})
}

// StorePieceRequest -- Upload Piece to Server
func (client *Client) StorePieceRequest(id string, ttl int64) (rv io.WriteCloser, err error) {
	defer mon.Task()(nil)(&err)

	stream, err := client.route.Store(client.ctx)
	if err != nil {
		return nil, err
	}

	// SSend preliminary data
	if err := stream.Send(&pb.PieceStore{Id: id, Ttl: ttl}); err != nil {
		stream.CloseAndRecv()
		return nil, fmt.Errorf("%v.Send() = %v", stream, err)
	}

	return &StreamWriter{stream: stream}, err
}

// RetrievePieceRequest -- Begin Download Piece from Server
func (client *Client) RetrievePieceRequest(id string, offset int64, length int64) (rv io.ReadCloser, err error) {
	defer mon.Task()(nil)(&err)
	stream, err := client.route.Retrieve(client.ctx, &pb.PieceRetrieval{Id: id, Size: length, Offset: offset})
	if err != nil {
		return nil, err
	}

	return NewStreamReader(stream), nil
}

// DeletePieceRequest -- Delete Piece From Server
func (client *Client) DeletePieceRequest(id string) (err error) {
	defer mon.Task()(nil)(&err)
	reply, err := client.route.Delete(client.ctx, &pb.PieceDelete{Id: id})
	if err != nil {
		return err
	}
	log.Printf("Route summary : %v", reply)
	return nil
}
