package pgcopy

import (
	"context"
	"database/sql/driver"
	"net/url"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

var ctx = context.Background()

// Conn -
type Conn struct {
	cn           *pgx.Conn
	fn           func([]driver.Value) error
	msg          *pglogrepl.RelationMessage
	table, sheme string
	readHead     bool
}

// New -
func New(uri, sheme, table string) (*Conn, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	param := url.Values{}
	param.Add("sslmode", "require")
	param.Add("application_name", "pgcopy")
	u.RawQuery = param.Encode()

	c := Conn{
		sheme: sheme,
		table: table,
	}
	c.cn, err = pgx.Connect(ctx, u.String())
	if err != nil {
		return nil, err
	}
	c.msg, err = c.RelationMessage()
	return &c, err
}

// Exec -
func (c *Conn) Exec(sql string, args ...any) error {
	_, err := c.cn.Exec(ctx, sql, args...)
	return err
}

// Close -
func (c *Conn) Close() error {
	if c.cn != nil {
		return c.cn.Close(ctx)
	}
	return nil
}
