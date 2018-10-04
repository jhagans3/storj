// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package jobs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"storj.io/storj/pkg/repeat"
)

type inc struct {
	id  string
	num int
}

func (s *inc) Name() string {
	return s.id
}
func (s *inc) State() string {
	return fmt.Sprintf("%v", s.num)
}
func (s *inc) Repeat() (bool, error) {
	// operation
	s.num = s.num + 1
	// termination condition
	if s.num >= 10 {
		return false, nil
	}

	return true, nil
}

func TestCountUpToTen(t *testing.T) {
	var add = inc{
		id:  "Ten",
		num: 1,
	}

	errors := []string{}
	jobs := []repeat.Repeater{&add}

	assert.Equal(t, "1", add.State())
	repeat.Repeat(jobs, errors)
	assert.Equal(t, "10", add.State())
}

type dec struct {
	id  string
	num int
}

func (s *dec) Name() string {
	return s.id
}
func (s *dec) State() string {
	return fmt.Sprintf("%v", s.num)
}
func (s *dec) Repeat() (bool, error) {
	// operation
	s.num = s.num - 1
	// termination condition
	if s.num <= 0 {
		return false, nil
	}

	return true, nil
}

func TestCountDownToZero(t *testing.T) {
	var sub = dec{
		id:  "Ten",
		num: 10,
	}

	errors := []string{}
	jobs := []repeat.Repeater{&sub}

	assert.Equal(t, "10", sub.State())
	repeat.Repeat(jobs, errors)
	assert.Equal(t, "0", sub.State())
}

func TestUpAndDown(t *testing.T) {
	var add = inc{
		id:  "Ten",
		num: 0,
	}
	var sub = dec{
		id:  "Ten",
		num: 10,
	}

	errors := []string{}
	jobs := []repeat.Repeater{&add, &sub}

	assert.Equal(t, "0", add.State())
	assert.Equal(t, "10", sub.State())
	repeat.Repeat(jobs, errors)
	assert.Equal(t, "10", add.State())
	assert.Equal(t, "0", sub.State())
}
