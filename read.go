package pgcopy

// Write - io.Writer
func (c *Conn) Write(src []byte) (n int, err error) {
	return
}

func (c *Conn) Read(sql string) {
	c.cn.PgConn().CopyTo(ctx, c, "COPY ("+sql+") TO STDOUT WITH BINARY;")
}
