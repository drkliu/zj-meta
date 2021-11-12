package meta

import (
	"bytes"
	"strconv"

	//"fmt"

	"text/template"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

const SQL_CREATE_TABLE = `
insert into META_TABLE (t_name) values (?)
`
const SQL_SELECT_ALL_TABLES = `
select t_id,t_name,c_id,c_name, type,is_primary_key,is_auto_increment,is_nullable,is_unique,is_index,is_unique_index,is_binary,is_unsigned,is_zero,is_date,is_time,is_timestamp,is_time_stampz,length,decimal_value,default_value,on_update,comment from META_TABLE left join META_COLUMN on t_id=table_id
`
const SQL_SELECT__TABLE_BY_ID = `
select t_id,t_name,c_id,c_name, type,is_primary_key,is_auto_increment,is_nullable,is_unique,is_index,is_unique_index,is_binary,is_unsigned,is_zero,is_date,is_time,is_timestamp,is_time_stampz,length,decimal_value,default_value,on_update,comment from META_TABLE left join META_COLUMN on t_id=table_id
 where t_id=?
`
const SQL_CREATE_COLUMN = `
insert into META_COLUMN (
	c_name, type,is_primary_key,is_auto_increment,is_nullable,is_unique,is_index,is_unique_index,is_binary,is_unsigned,is_zero,is_date,is_time,is_timestamp,is_time_stampz,length,decimal_value,default_value,on_update,comment,table_id) 
	values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`
const SQL_SELECT_ALL= `
select * from ?
`

type Repository interface {
	CreateMetaTableIfNotExists(*MetaTable) error

	SelectMetaTables() ([]*MetaTable, error)
	SelectMetaTableById(int) (*MetaTable, error)
	CreateTable(*MetaTable) error
	SelectAll(*MetaTable) ([]*map[string]interface{},error)
	//SelectTableById(int) (*MetaTable, error)
	//CreateTables(MetaTable)
}
type repository struct {
	db *sqlx.DB
}

func NewRepository(db *sqlx.DB) Repository {
	return &repository{db}
}
func (r *repository) CreateMetaTableIfNotExists(metaTable *MetaTable) error {
	tx := r.db.MustBegin()
	resultTable := tx.MustExec(SQL_CREATE_TABLE, metaTable.TableName)
	lastInsertId, err := resultTable.LastInsertId()
	log.Info().Msgf("lastInsertId:%d", lastInsertId)
	if err != nil {
		tx.Rollback()
		return err
	}
	for _, column := range metaTable.Columns {
		tx.MustExec(SQL_CREATE_COLUMN, column.ColumnName, column.Type, column.IsPrimaryKey, column.IsAutoIncrement, column.IsNullable, column.IsUnique, column.IsIndex, column.IsUniqueIndex, column.IsBinary, column.IsUnsigned, column.IsZero, column.IsDate, column.IsTime, column.IsTimeStamp, column.IsTimeStampz, column.Length, column.Decimal, column.Default, column.OnUpdate, column.Comment, lastInsertId)
	}
	tx.Commit()
	return nil
}

func (r *repository) SelectMetaTables() ([]*MetaTable, error) {
	tables := []*MetaTable{}
	rows, err := r.db.Query(SQL_SELECT_ALL_TABLES)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tableMap := make(map[int]*MetaTable)
	columnMap := make(map[int][]*MetaColumn)
	tableIds := []int{}
	for rows.Next() {
		column := &MetaColumn{}
		table := MetaTable{}
		if err := rows.Scan(&table.TableId, &table.TableName,
			&column.ColumnId, &column.ColumnName, &column.Type, &column.IsPrimaryKey,
			&column.IsAutoIncrement, &column.IsNullable, &column.IsUnique, &column.IsIndex,
			&column.IsUniqueIndex, &column.IsBinary, &column.IsUnsigned,
			&column.IsZero, &column.IsDate, &column.IsTime, &column.IsTimeStamp, &column.IsTimeStampz,
			&column.Length, &column.Decimal, &column.Default, &column.OnUpdate, &column.Comment); err != nil {
			return tables, err
		}
		if tableMap[table.TableId] == nil {
			tableMap[table.TableId] = &table
			tableIds = append(tableIds, table.TableId)
		}
		if columnMap[table.TableId] == nil {
			columnMap[table.TableId] = []*MetaColumn{}
			columnMap[table.TableId] = append(columnMap[table.TableId], column)
		} else {
			columnMap[table.TableId] = append(columnMap[table.TableId], column)
		}
	}
	for _, tableId:= range tableIds {
		table:=tableMap[tableId]
		columns := columnMap[table.TableId]
		if len(columns) > 0 {
			table.Columns = columns
			tables = append(tables, table)
		}
	}
	if err := rows.Err(); err != nil {
		return tables, err
	}
	return tables, err
}
func (r *repository) SelectMetaTableById(tableId int) (*MetaTable, error) {
	table := MetaTable{}
	rows, err := r.db.Query(SQL_SELECT__TABLE_BY_ID, tableId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := []*MetaColumn{}

	for rows.Next() {
		column := &MetaColumn{}
		if err := rows.Scan(&table.TableId, &table.TableName,
			&column.ColumnId, &column.ColumnName, &column.Type, &column.IsPrimaryKey,
			&column.IsAutoIncrement, &column.IsNullable, &column.IsUnique, &column.IsIndex,
			&column.IsUniqueIndex, &column.IsBinary, &column.IsUnsigned,
			&column.IsZero, &column.IsDate, &column.IsTime, &column.IsTimeStamp, &column.IsTimeStampz,
			&column.Length, &column.Decimal, &column.Default, &column.OnUpdate, &column.Comment); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	table.Columns = columns
	return &table, nil
}


func (r *repository) CreateTable(metaTable *MetaTable) error {
	
	te, err := template.ParseFiles("../config/template/create_table.tmpl")
	if err != nil {
		panic(err)
	}
	data := make(map[string]interface{}, 2)
	data["lastColumnIndex"] = len(metaTable.Columns) - 1
	data["table"] = &metaTable
	buf := new(bytes.Buffer)
	err = te.Execute(buf, data)

	log.Logger.Printf("%s", buf.String())
	if err != nil {
		panic(err)
	}
	return nil
}

func (r *repository) SelectAll(metaTable *MetaTable) ([]*map[string]interface{},error) {
	var sql bytes.Buffer
	sql.WriteString("select ")
	 
	for index, element := range metaTable.Columns {
		if index>0 {
			sql.WriteString(",")
		}
		sql.WriteString(element.ColumnName)
	}
	sql.WriteString(" from ")
	sql.WriteString(metaTable.TableName)
	rows, err := r.db.Query(sql.String())
	if err != nil {
		return nil, err
	}
	 
	defer rows.Close()
	var data []*map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(metaTable.Columns))
		for i := range values {
			values[i] = new(interface{})
		}
		err = rows.Scan(values...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for i,column := range metaTable.Columns {
			v := *(values[i].(*interface{}))
			if v==nil{
				row[column.ColumnName] = nil
				continue
			}
			switch column.Type {
				case "tinyint":
					if column.Length==1 {
						v,err := strconv.Atoi(string(v.([]uint8)))
						if err != nil {
							return nil,err
						}
						if v<1{
							row[column.ColumnName] = false
						}else{
							row[column.ColumnName] = true
						}
					}
				case "int":
					v,err := strconv.Atoi(string(v.([]uint8)))
					if err != nil {
						return nil,err
					}
					row[column.ColumnName]=v
				case "decimal":
					v,err := strconv.ParseFloat(string(v.([]uint8)),64)
					if err != nil {
						return nil,err
					}
					row[column.ColumnName]=v
				case "varchar":
					row[column.ColumnName] = string(v.([]uint8))
				default:
					row[column.ColumnName] = string(v.([]uint8))
			}
			
		}
		data = append(data, &row)
	}
	 
	
	// for rows.Next() {
	// 	rowData:=make(map[string]interface{},len(metaTable.Columns))
	// 	if err := rows.Scan(&rowData); err != nil {
	// 		return nil, err
	// 	}
	// 	data = append(data, &rowData)
	// }
	 
	if err!=nil {
		return nil,err
	}
	return data, rows.Err()
}