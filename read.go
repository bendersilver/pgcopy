package pgcopy

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/jackc/pglogrepl"
)

var signature = []byte{0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00}

// Write - io.Writer
func (c *Conn) Write(src []byte) (n int, err error) {
	if !c.readHead {
		// https://postgrespro.ru/docs/postgresql/14/sql-copy#id-1.9.3.55.9.4.5
		// Заголовок файла содержит 15 байт фиксированных полей, за которыми следует область
		// расширения заголовка переменной длины. Фиксированные поля:
		// Сигнатура
		//		Последовательность из 11 байт PGCOPY\n\377\r\n\0
		// Поле флагов 32 бит
		// Длина области расширения заголовка 32 бит
		if !bytes.HasPrefix(signature, signature) {
			return 0, fmt.Errorf("invalid file signature: %s", signature)
		}
		src = src[19:]
		c.readHead = true
	}
	c.msg.Tuple = new(pglogrepl.TupleData)
	// Каждая запись начинается с 16-битного целого числа, определяющего количество полей в записи.
	c.msg.Tuple.ColumnNum = binary.BigEndian.Uint16(src[:2])

	// Окончание файла состоит из 16-битного целого, содержащего -1.
	// Это позволяет легко отличить его от счётчика полей в записи.
	// EOF
	if int16(c.msg.Tuple.ColumnNum) == -1 {
		return
	}
	c.msg.SetType(pglogrepl.MessageTypeInsert)
	n, err = c.msg.Tuple.Decode(src)
	c.fn(&c.msg)
	return
}

func (c *Conn) Read(sql string, f func(*pglogrepl.InsertMessage)) error {
	c.msg.SetType(pglogrepl.MessageTypeInsert)
	c.fn = f
	_, err := c.cn.PgConn().CopyTo(ctx, c, "COPY ("+sql+") TO STDOUT WITH BINARY;")
	return err
}
