package tpch

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pingcap/go-tpc/pkg/load"
	"github.com/pingcap/go-tpc/tpch/dbgen"
)

type sqlLoader struct {
	*load.SQLBatchLoader
	context.Context
}

func (s *sqlLoader) InsertValue(value string) error {
	return s.SQLBatchLoader.InsertValue(s.Context, []string{value})
}

func (s *sqlLoader) Flush() error {
	return s.SQLBatchLoader.Flush(s.Context)
}

type orderLoader struct {
	sqlLoader
}

func (o *orderLoader) Load(item interface{}) error {
	order := item.(*dbgen.Order)
	v := fmt.Sprintf("(%d,%d,'%c','%s','%s','%s','%s',%d,'%s')",
		order.OKey,
		order.CustKey,
		order.Status,
		dbgen.FmtMoney(order.TotalPrice),
		order.Date,
		order.OrderPriority,
		order.Clerk,
		order.ShipPriority,
		order.Comment)
	return o.InsertValue(v)
}

type custLoader struct {
	sqlLoader
}

func (c *custLoader) Load(item interface{}) error {
	cust := item.(*dbgen.Cust)
	v := fmt.Sprintf("(%d,'%s','%s',%d,'%s','%s','%s','%s')",
		cust.CustKey,
		cust.Name,
		cust.Address,
		cust.NationCode,
		cust.Phone,
		dbgen.FmtMoney(cust.Acctbal),
		cust.MktSegment,
		cust.Comment)
	return c.InsertValue(v)
}

type lineItemloader struct {
	sqlLoader
}

func (l *lineItemloader) Load(item interface{}) error {
	order := item.(*dbgen.Order)
	for _, line := range order.Lines {
		v := fmt.Sprintf("(%d,%d,%d,%d,%d,'%s','%s','%s','%c','%c','%s','%s','%s','%s','%s','%s')",
			line.OKey,
			line.PartKey,
			line.SuppKey,
			line.LCnt,
			line.Quantity,
			dbgen.FmtMoney(line.EPrice),
			dbgen.FmtMoney(line.Discount),
			dbgen.FmtMoney(line.Tax),
			line.RFlag,
			line.LStatus,
			line.SDate,
			line.CDate,
			line.RDate,
			line.ShipInstruct,
			line.ShipMode,
			line.Comment,
		)
		if err := l.InsertValue(v); err != nil {
			return nil
		}
	}
	return nil
}

type nationLoader struct {
	sqlLoader
}

func (n *nationLoader) Load(item interface{}) error {
	nation := item.(*dbgen.Nation)
	v := fmt.Sprintf("(%d,'%s',%d,'%s')",
		nation.Code,
		nation.Text,
		nation.Join,
		nation.Comment)
	return n.InsertValue(v)
}

type partLoader struct {
	sqlLoader
}

func (p *partLoader) Load(item interface{}) error {
	part := item.(*dbgen.Part)
	v := fmt.Sprintf("(%d,'%s','%s','%s','%s',%d,'%s','%s','%s')",
		part.PartKey,
		part.Name,
		part.Mfgr,
		part.Brand,
		part.Type,
		part.Size,
		part.Container,
		dbgen.FmtMoney(part.RetailPrice),
		part.Comment)
	return p.InsertValue(v)
}

type partSuppLoader struct {
	sqlLoader
}

func (p *partSuppLoader) Load(item interface{}) error {
	part := item.(*dbgen.Part)
	for _, supp := range part.S {
		v := fmt.Sprintf("(%d,%d,%d,'%s','%s')",
			supp.PartKey,
			supp.SuppKey,
			supp.Qty,
			dbgen.FmtMoney(supp.SCost),
			supp.Comment)
		if err := p.InsertValue(v); err != nil {
			return err
		}
	}
	return nil
}

type suppLoader struct {
	sqlLoader
}

func (s *suppLoader) Load(item interface{}) error {
	supp := item.(*dbgen.Supp)
	v := fmt.Sprintf("(%d,'%s','%s',%d,'%s','%s','%s')",
		supp.SuppKey,
		supp.Name,
		supp.Address,
		supp.NationCode,
		supp.Phone,
		dbgen.FmtMoney(supp.Acctbal),
		supp.Comment)
	return s.InsertValue(v)
}

type regionLoader struct {
	sqlLoader
}

func (r *regionLoader) Load(item interface{}) error {
	region := item.(*dbgen.Region)
	v := fmt.Sprintf("(%d,'%s','%s')",
		region.Code,
		region.Text,
		region.Comment)
	return r.InsertValue(v)
}

func NewOrderLoader(ctx context.Context, conn *sql.Conn) *orderLoader {
	return &orderLoader{sqlLoader{load.NewSQLBatchLoader(conn,
		`INSERT INTO orders (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) VALUES `),
		ctx}}
}
func NewLineItemLoader(ctx context.Context, conn *sql.Conn) *lineItemloader {
	return &lineItemloader{sqlLoader{load.NewSQLBatchLoader(conn,
		`INSERT INTO lineitem (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) VALUES `),
		ctx}}
}
func NewCustLoader(ctx context.Context, conn *sql.Conn) *custLoader {
	return &custLoader{sqlLoader{load.NewSQLBatchLoader(conn,
		`INSERT INTO customer (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) VALUES `),
		ctx}}
}
func NewPartLoader(ctx context.Context, conn *sql.Conn) *partLoader {
	return &partLoader{sqlLoader{load.NewSQLBatchLoader(conn,
		`INSERT INTO part (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT) VALUES `),
		ctx}}
}
func NewPartSuppLoader(ctx context.Context, conn *sql.Conn) *partSuppLoader {
	return &partSuppLoader{sqlLoader{load.NewSQLBatchLoader(conn,
		`INSERT INTO partsupp (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) VALUES `),
		ctx}}
}
func NewSuppLoader(ctx context.Context, conn *sql.Conn) *suppLoader {
	return &suppLoader{sqlLoader{load.NewSQLBatchLoader(conn,
		`INSERT INTO supplier (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT) VALUES `),
		ctx}}
}
func NewNationLoader(ctx context.Context, conn *sql.Conn) *nationLoader {
	return &nationLoader{sqlLoader{load.NewSQLBatchLoader(conn,
		`INSERT INTO nation (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) VALUES `),
		ctx}}
}
func NewRegionLoader(ctx context.Context, conn *sql.Conn) *regionLoader {
	return &regionLoader{sqlLoader{load.NewSQLBatchLoader(conn,
		`INSERT INTO region (R_REGIONKEY, R_NAME, R_COMMENT) VALUES `),
		ctx}}
}
