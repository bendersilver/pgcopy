package main

import (
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

	err = c.Read("SELECT * FROM pb._ev", func(im *pglogrepl.InsertMessage) {
		log.Println(im)
	})
	if err != nil {
		log.Fatal(err)
	}

}
