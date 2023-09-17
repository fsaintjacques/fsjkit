package internal

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type Metadata map[string]string

func (m *Metadata) Value() (driver.Value, error) {
	if m == nil {
		return sql.NullString{}, nil
	}
	return json.Marshal(m)
}

func (m *Metadata) Scan(value any) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, &m)
}
