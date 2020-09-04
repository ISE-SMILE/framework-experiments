package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bcongdon/corral"
)

type q6 struct {
	before    time.Time
	after     time.Time
	discound  float64
	quantitiy int64
}

type lineitem int

const (
	L_ORDERKEY lineitem = iota
	L_PARTKEY
	L_SUPPKEY
	L_LINENUMBER
	L_QUANTITY
	L_EXTENDEDPRICE
	L_DISCOUNT
	L_TAX
	L_RETURNFLAG
	L_LINESTATUS
	L_SHIPDATE
	L_COMMITDATE
	L_RECEIPTDATE
	L_SHIPINSTRUCT
	L_SHIPMODE
	L_COMMENT
)

/**sql
	select
			sum(l_extendedprice * l_discount) as revenue
	from
			lineitem
	where
			l_shipdate >= date '1994-01-01'
			and l_shipdate < date '1995-01-01'
			and l_discount between 0.06 - 0.01 and 0.06 + 0.01
			and l_quantity < 24
**/

const verbose = true

func (w q6) Map(key, value string, emitter corral.Emitter) {
	line := strings.Split(value, "|")
	if len(line) < 15 {
		return
	}
	//first the where clause
	quantitiy, _ := strconv.ParseInt(line[L_QUANTITY], 10, 32)

	discound, _ := strconv.ParseFloat(line[L_DISCOUNT], 32)

	shipdate, _ := time.Parse("2006-01-02", line[L_SHIPDATE])

	extendedprice, _ := strconv.ParseFloat(line[L_EXTENDEDPRICE], 32)

	where := quantitiy < w.quantitiy && discound <= w.discound+0.01 && discound >= w.discound-0.01 && shipdate.Before(w.before) && shipdate.After(w.after)
	if where {
		prod := discound * extendedprice
		_ = emitter.Emit("revenue", fmt.Sprintf("%f", prod))
	}
}

func (w q6) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	sum := 0.
	for prod := range values.Iter() {
		tmp, _ := strconv.ParseFloat(prod, 32)
		sum += tmp
	}
	_ = emitter.Emit("revenue", fmt.Sprintf("%f", sum))
}

func main() {
	before, _ := time.Parse("2006-01-02", "1995-01-01")
	after, _ := time.Parse("2006-01-02", "1994-01-01")

	wc := q6{
		discound:  0.05,
		quantitiy: 24,
		before:    before,
		after:     after,
	}
	job := corral.NewJob(wc, wc)

	driver := corral.NewDriver(job)
	driver.Main()
}
