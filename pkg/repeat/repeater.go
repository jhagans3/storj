// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package repeat

// Repeater behavior for loop
type Repeater interface {
	Repeat() (bool, error)
	Name() string
	State() string
}
