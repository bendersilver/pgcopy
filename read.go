package pgcopy

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

var signature = []byte{0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00}
var mi = pgtype.NewMap()

// Write - io.Writer
func (c *Conn) Write(src []byte) (n int, err error) {
	n = len(src)

	var low int
	if !c.readHead {
		// https://postgrespro.ru/docs/postgresql/14/sql-copy#id-1.9.3.55.9.4.5
		// Заголовок файла содержит 15 байт фиксированных полей, за которыми следует область
		// расширения заголовка переменной длины. Фиксированные поля:
		// Сигнатура
		//		Последовательность из 11 байт PGCOPY\n\377\r\n\0
		// Поле флагов 32 бит
		// Длина области расширения заголовка 32 бит
		if !bytes.HasPrefix(src, signature) {
			return 0, fmt.Errorf("invalid file signature: %s", signature)
		}
		low = 19
		c.readHead = true
	}
	// Каждая запись начинается с 16-битного целого числа, определяющего количество полей в записи.
	columnNum := decodeInt16(src[low:], &low)
	// Окончание файла состоит из 16-битного целого, содержащего -1.
	// Это позволяет легко отличить его от счётчика полей в записи.
	// EOF
	if columnNum == -1 {
		return
	}
	vals := make([]driver.Value, columnNum)
	for i := range vals {
		columnLen := decodeInt32(src[low:], &low)
		if columnLen == -1 {
			vals[i] = nil
			continue
		}
		low += int(columnLen)
		vals[i], err = decodeColumn(c.msg.Columns[i].DataType, src[low-int(columnLen):low])
		if err != nil {
			return 0, err
		}
	}

	return n, c.fn(vals)
}

func (c *Conn) Read(sql string, f func([]driver.Value) error) error {
	c.msg.SetType(pglogrepl.MessageTypeInsert)
	c.fn = f
	_, err := c.cn.PgConn().CopyTo(ctx, c, "COPY ("+sql+") TO STDOUT WITH BINARY;")
	return err
}

func decodeInt32(src []byte, counter *int) int32 {
	*counter += 4
	return int32(binary.BigEndian.Uint32(src))
}

func decodeInt16(src []byte, counter *int) int16 {
	*counter += 2
	return int16(binary.BigEndian.Uint16(src))
}

func decodeColumn(oid uint32, data []byte) (v driver.Value, err error) {
	if dt, ok := mi.TypeForOID(oid); ok {
		dv, err := dt.Codec.DecodeDatabaseSQLValue(mi, oid, pgtype.BinaryFormatCode, data)
		if err != nil {
			return nil, err
		}
		return dv, nil
	}
	return decodeColumn(17, data)
}
