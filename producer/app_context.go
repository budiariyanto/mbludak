package producer

import (
	"github.com/google/uuid"
	"math/rand"
	"regexp"
	"strings"
	"time"
)

type AppContext struct {
	Brokers string
	Topic string
	File string
	Format string
	Threads int64
	Duration time.Duration
	Username string
	Password string
	RandomFields []RandomField
}

func (c *AppContext) SetRandomFields(fields string) error {
	arrField := strings.Split(fields, ",")
	for _, v := range arrField {
		match, _ := regexp.MatchString("[a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+,?", v)
		if match {
			fieldSpec := strings.Split(v, ":")
			randomField := RandomField{
				Name: fieldSpec[0],
				Type: fieldSpec[1],
			}

			c.RandomFields = append(c.RandomFields, randomField)
		}
	}

	return nil
}

type RandomField struct {
	Name string
	Type string
}

func (r RandomField) Generate() interface{} {
	switch r.Type {
	case "uuid":
		uuid, _ := uuid.NewRandom()
		return uuid.String()
	case "number":
		source := rand.NewSource(time.Now().UnixNano())
		return rand.New(source).Int()
	default:
		return nil
	}
}