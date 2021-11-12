package meta

import (
	"bytes"
	"strconv"
)

type MetaDatabase struct {
	Name   string
	Tables []*MetaTable
}

type MetaTable struct {
	TableId   int    `db:"t_id"`
	TableName string `db:"t_name"`
	Columns   []*MetaColumn
	Indexes   []*Index
}

type MetaColumn struct {
	ColumnId        int    `db:"c_id"`
	ColumnName      string `db:"c_name"`
	Type            string
	IsPrimaryKey    bool
	IsAutoIncrement bool
	IsNullable      bool
	IsUnique        bool
	IsIndex         bool
	IsUniqueIndex   bool
	IsBinary        bool
	IsUnsigned      bool
	IsZero          bool
	IsDate          bool
	IsTime          bool
	IsTimeStamp     bool
	IsTimeStampz    bool
	Length          int
	Decimal         int
	Default         string
	OnUpdate        string
	Comment         string
}

type Index struct {
	Name     string
	Type     string
	Colmuns  []string
	IsUnique bool
}
type ForeignKey struct {
	Name       string
	Columns    []string
	RefTable   string
	RefColumns []string
	OnDelete   string
	OnUpdate   string
}

func (db *MetaDatabase) GetTable(name string) *MetaTable {
	for _, t := range db.Tables {
		if t.TableName == name {
			return t
		}
	}
	return nil
}

func (t *MetaTable) GetPrimaryKeys() []string {
	var keys []string
	for _, c := range t.Columns {
		if c.IsPrimaryKey {
			keys = append(keys, c.ColumnName)
		}
	}
	return keys
}
func (t *MetaTable) HasPrimaryKey() bool {
	for _, c := range t.Columns {
		if c.IsPrimaryKey {
			return true
		}
	}
	return false
}

func (t *MetaTable) SqlPrimaryKey() string {
	 
	var keyBuffer bytes.Buffer
	pkCount := 0
	for _, c := range t.Columns {
		if c.IsPrimaryKey {
			if pkCount > 0 {
				keyBuffer.WriteString(",")
			}
			keyBuffer.WriteString("`")
			keyBuffer.WriteString(c.ColumnName)
			keyBuffer.WriteString("`")
			pkCount++
		}
	}
	if pkCount < 1 {
		return ""
	}
	var b bytes.Buffer
	b.WriteString("PRIMARY KEY(")
	b.WriteString(keyBuffer.String())
	b.WriteString(")")
	return b.String() 
}

func (c *MetaColumn) Sql() string {
	var b bytes.Buffer
	b.WriteString("`")
	b.WriteString(c.ColumnName)
	b.WriteString("`")
	b.WriteString(" ")
	b.WriteString(c.Type)
	if c.IsPrimaryKey {
		b.WriteString(" NOT NULL")
	}
	if c.IsAutoIncrement {
		b.WriteString(" AUTO_INCREMENT")
	}
	if c.IsNullable {
		b.WriteString(" NULL")
	}
	if c.IsUnique {
		b.WriteString(" UNIQUE")
	}
	if c.IsIndex {
		b.WriteString(" INDEX")
	}
	if c.IsUniqueIndex {
		b.WriteString(" UNIQUE INDEX")
	}
	if c.IsBinary {
		b.WriteString(" BINARY")
	}
	if c.IsUnsigned {
		b.WriteString(" UNSIGNED")
	}
	if c.IsZero {
		b.WriteString(" ZEROFILL")
	}
	if c.IsDate {
		b.WriteString(" DATE")
	}
	if c.IsTime {
		b.WriteString(" TIME")
	}
	if c.IsTimeStamp {
		b.WriteString(" TIMESTAMP")
	}
	if c.IsTimeStampz {
		b.WriteString(" TIMESTAMP(6)")
	}
	if c.Length > 0 {
		b.WriteString("(")
		b.WriteString(strconv.Itoa(c.Length))
		b.WriteString(")")
	}
	if c.Decimal > 0 {
		b.WriteString("(")
		b.WriteString(strconv.Itoa(c.Length))
		b.WriteString(",")
		b.WriteString(strconv.Itoa(c.Decimal))
		b.WriteString(")")
	}
	return b.String()
}
