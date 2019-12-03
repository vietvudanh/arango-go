package arango

import (
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"log"
)

func GetClient() driver.Client {
	//
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{"http://localhost:8529"},
	})
	if err != nil {
		log.Panic("cannot create http connection")
	}
	c, err := driver.NewClient(driver.ClientConfig{
		Connection: conn,
		Authentication: driver.BasicAuthentication("root", ""),
	})
	if err != nil {
		log.Panic("cannot create client")
	}

	return c
}
