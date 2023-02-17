package main

import (
	"database/sql/driver"
	"log"
	"os"

	"github.com/bendersilver/pgcopy"
)

func main() {
	c, err := pgcopy.New(os.Getenv("PG_URL"), "pb", "users")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = c.Read("SELECT * FROM pb.users", func(vals []driver.Value) error {
		// decodeTuple(im.Tuple)
		log.Println(vals)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

}
