package pgcopy

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

var ctx = context.Background()

// Conn -
type Conn struct {
	cn           *pgx.Conn
	fn           func(*pglogrepl.InsertMessage) error
	msg          pglogrepl.InsertMessage
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
	return &c, c.cn.QueryRow(
		ctx, fmt.Sprintf("SELECT '%s.%s'::regclass::INT;", sheme, table),
	).Scan(&c.msg.RelationID)
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
