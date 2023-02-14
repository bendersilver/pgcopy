package pgcopy

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

// RelationMessage -
func (c *Conn) RelationMessage() (*pglogrepl.RelationMessage, error) {
	msg := pglogrepl.RelationMessage{
		RelationName: c.table,
		Namespace:    c.sheme,
	}

	err := c.cn.QueryRow(ctx, fmt.Sprintf(`
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
		`, c.sheme, c.table)).Scan(&msg.RelationID, &msg.ColumnNum, &msg.Columns)
	if err != nil {
		return nil, err
	}

	return &msg, nil
}
