package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/fraugster/parquet-go/floor/interfaces"
	log "github.com/sirupsen/logrus"
)

type ParquetUser struct {
	Id          int       `json:"id"`
	FirstName   string    `json:"firstName"`
	LastName    string    `json:"lastName"`
	Role        string    `json:"role"`
	LastUpdated time.Time `json:"lastUpdated"`
}

func (r *ParquetUser) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
	id, err := obj.GetField("id").Int32()

	if err != nil {
		return errors.New(fmt.Sprintf("error unmarshalling row on field (id)"))
	}

	firstName, err := obj.GetField("firstName").ByteArray()

	if err != nil {
		return errors.New(fmt.Sprintf("error unmarshalling row on field (firstName)"))
	}

	lastName, err := obj.GetField("lastName").ByteArray()

	if err != nil {
		return errors.New(fmt.Sprintf("error unmarshalling row on field (lastName)"))
	}

	role, err := obj.GetField("role").ByteArray()

	if err != nil {
		return errors.New(fmt.Sprintf("error unmarshalling row on field (role)"))
	}

	// note this is a time.Time but comes across as an Int64
	lastUpdated, err := obj.GetField("lastUpdated").Int64()

	if err != nil {
		return errors.New(fmt.Sprintf("error unmarshalling row on field (lastUpdated)"))
	}

	parsed := time.UnixMicro(lastUpdated)

	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("error parsing time")
		return errors.New(fmt.Sprintf("(lastUpdated) is not in the right format"))
	}

	r.Id = int(id)
	r.FirstName = string(firstName)
	r.LastName = string(lastName)
	r.Role = string(role)
	r.LastUpdated = parsed
	return nil
}
