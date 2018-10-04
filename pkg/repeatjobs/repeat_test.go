// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package repeatjobs

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"storj.io/storj/pkg/datarepair/checker"
	"storj.io/storj/pkg/datarepair/repairer"
	"storj.io/storj/pkg/repeat"
)

var (
	ctx = context.Background()
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

type checkerJob struct {
	id  string
	num int
}

func (c *checkerJob) Name() string {
	return c.id
}
func (c *checkerJob) State() string {
	return fmt.Sprintf("%v", c.num)
}
func (c *checkerJob) Repeat() (bool, error) {
	// operation
	conf := checker.Config{}
	conf.Run(ctx)
	c.num = c.num + 1
	// termination condition
	if c.num > 5 {
		return false, nil
	}
	return true, nil

}

func TestChecker(t *testing.T) {
	var check = checkerJob{
		id:  "Shard Checker",
		num: 0,
	}

	errors := []string{}
	jobs := []repeat.Repeater{&check}
	repeat.Repeat(jobs, errors)
	assert.Equal(t, 6, check.num)

}

type repairJob struct {
	id  string
	num int
}

func (r *repairJob) Name() string {
	return r.id
}
func (r *repairJob) State() string {
	return fmt.Sprintf("%v", r.num)
}
func (r *repairJob) Repeat() (bool, error) {
	// operation
	conf := repairer.Config{
		// maxRepair: 100,
	}
	conf.Run(ctx)
	r.num = r.num + 1
	// termination condition
	if r.num > 5 {
		return false, nil
	}
	return true, nil

}

func TestRepair(t *testing.T) {
	var repair = checkerJob{
		id:  "Shard Repair",
		num: 0,
	}

	errors := []string{}
	jobs := []repeat.Repeater{&repair}
	repeat.Repeat(jobs, errors)
	assert.Equal(t, 6, repair.num)

}
