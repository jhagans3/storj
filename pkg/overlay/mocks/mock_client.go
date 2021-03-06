// Code generated by MockGen. DO NOT EDIT.
// Source: storj.io/storj/pkg/overlay (interfaces: Client)

// Package mock_overlay is a generated GoMock package.
package mock_overlay

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
	dht "storj.io/storj/pkg/dht"
	overlay "storj.io/storj/protos/overlay"
)

// MockClient is a mock of Client interface
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// BulkLookup mocks base method
func (m *MockClient) BulkLookup(arg0 context.Context, arg1 []dht.NodeID) ([]*overlay.Node, error) {
	ret := m.ctrl.Call(m, "BulkLookup", arg0, arg1)
	ret0, _ := ret[0].([]*overlay.Node)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BulkLookup indicates an expected call of BulkLookup
func (mr *MockClientMockRecorder) BulkLookup(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BulkLookup", reflect.TypeOf((*MockClient)(nil).BulkLookup), arg0, arg1)
}

// Choose mocks base method
func (m *MockClient) Choose(arg0 context.Context, arg1 int, arg2 int64) ([]*overlay.Node, error) {
	ret := m.ctrl.Call(m, "Choose", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*overlay.Node)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Choose indicates an expected call of Choose
func (mr *MockClientMockRecorder) Choose(arg0, arg1, arg2 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Choose", reflect.TypeOf((*MockClient)(nil).Choose), arg0, arg1, arg2)
}

// Lookup mocks base method
func (m *MockClient) Lookup(arg0 context.Context, arg1 dht.NodeID) (*overlay.Node, error) {
	ret := m.ctrl.Call(m, "Lookup", arg0, arg1)
	ret0, _ := ret[0].(*overlay.Node)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Lookup indicates an expected call of Lookup
func (mr *MockClientMockRecorder) Lookup(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Lookup", reflect.TypeOf((*MockClient)(nil).Lookup), arg0, arg1)
}
