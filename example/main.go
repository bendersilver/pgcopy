package main

import (
	"log"
	"os"

	"github.com/bendersilver/pgcopy"
)

func main() {
	c, err := pgcopy.New(os.Getenv("PG_URL"), "pb", "employee")
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(c.Read("SELECT * FROM pb.employee LIMIT 2"))

}
