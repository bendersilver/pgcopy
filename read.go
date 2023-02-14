package pgcopy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

var signature = []byte{0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00}

// Write - io.Writer
func (c *Conn) Write(src []byte) (n int, err error) {
	buf := bytes.NewBuffer(src)
	n = buf.Len()
	if !c.readSign {

		if !bytes.Equal(buf.Next(len(signature)), signature) {
			return 0, fmt.Errorf("invalid file signature: %s", signature)
		}
		log.Println(readInt32(buf))
		extension := make([]byte, readInt32(buf))

		if _, err := io.ReadFull(buf, extension); err != nil {
			return 0, fmt.Errorf("can't read header extension: %v", err)
		}
		log.Printf("%s", extension)
		c.readSign = true
	}

	tupleLen := readInt16(buf)
	// EOF
	if tupleLen == -1 {
		return
	}
	// vals := make([]driver.Value, tupleLen)
	for i := 0; i < int(tupleLen); i++ {
		colLen := readInt32(buf)
		log.Println(colLen)
		// column is nil
		if colLen == -1 {
			// vals[i] = nil
			continue
		}
		col := make([]byte, colLen)
		if _, err := io.ReadFull(buf, col); err != nil {
			return 0, fmt.Errorf("can't read column %v", err)
		}
		// vals[i], err = decodeColumn(pgtype.BinaryFormatCode, t.field[i].oid, col)
		// if err != nil {
		// 	return 0, err
		// }
	}

	return
}

func readInt32(r io.Reader) int32 {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(buf[:]))
}

func readInt16(r io.Reader) int16 {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(buf[:]))
}

func (c *Conn) Read(sqlCustom string) error {
	_, err := c.cn.PgConn().CopyTo(ctx, c, "COPY ("+sqlCustom+") TO STDOUT WITH BINARY;")
	return err
}
