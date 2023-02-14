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
	cn *pgx.Conn
	rm *pglogrepl.RelationMessage
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

	var c Conn

	c.cn, err = pgx.Connect(ctx, u.String())
	if err != nil {
		return nil, err
	}

	c.rm = &pglogrepl.RelationMessage{
		RelationName: table,
		Namespace:    sheme,
	}

	err = c.cn.QueryRow(ctx, fmt.Sprintf(`
			SELECT pa.attrelid, COUNT(*), json_agg(
				json_build_object(
					'Name', pa.attname,
					'DataType', pa.atttypid::INT,
					'TypeModifier', pa.atttypmod,
					'Flags', COALESCE(pi.indisprimary, FALSE)::INT
				) ORDER BY pa.attnum
			)
			FROM pg_attribute pa
			LEFT JOIN pg_index pi ON pa.attrelid = pi.indrelid AND pa.attnum = ANY(pi.indkey) AND pi.indisprimary IS TRUE
			WHERE pa.attrelid = '%s.%s'::regclass
				AND pa.attnum > 0
				AND NOT pa.attisdropped
			GROUP BY 1;
	`, sheme, table)).Scan(&c.rm.RelationID, &c.rm.ColumnNum, &c.rm.Columns)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

// Close -
func (c *Conn) Close() error {
	if c.cn != nil {
		return c.cn.Close(ctx)
	}
	return nil
}
