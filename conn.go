package pgcopy

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
)

var ctx = context.Background()

// Conn -
type Conn struct {
	cn           *pgx.Conn
	table, sheme string
	readSign     bool
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
	return &c, c.cn.QueryRow(ctx, fmt.Sprintf("SELECT '%s.%s'::regclass;", sheme, table)).Scan(nil)
}

// Close -
func (c *Conn) Close() error {
	if c.cn != nil {
		return c.cn.Close(ctx)
	}
	return nil
}
