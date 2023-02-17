package main

import (
	"fmt"
	"log"
	"os"

	"github.com/bendersilver/pgcopy"
	"github.com/jackc/pglogrepl"
)

func main() {
	c, err := pgcopy.New(os.Getenv("PG_URL"), "pb", "employee")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = c.Read("SELECT * FROM pb._ev", func(im *pglogrepl.InsertMessage) error {
		log.Println(im)
		return fmt.Errorf("err")
	})
	if err != nil {
		log.Fatal(err)
	}

}
