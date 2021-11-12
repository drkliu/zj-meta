package main

import (
	//	"database/sql"

	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"

	"time"

	"github.com/drkliu/zj-meta/internal/meta"
	"github.com/gin-contrib/logger"

	//"github.com/gin-contrib/requestid"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)
func main() {
	var rxURL = regexp.MustCompile(`^/regexp\d*`)
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if gin.IsDebugging() {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	log.Logger = log.Output(
		zerolog.ConsoleWriter{
			Out:     os.Stderr,
			NoColor: false,
		},
	)
	db, err := sqlx.Connect("mysql", "root:123abcABC@(127.0.0.1:3306)/zjmall-product")
    if err != nil {
        panic(err)
    }


	


	r := gin.Default()
	var t meta.MetaTable
	var cols=[]*meta.MetaColumn{
		{ColumnName: "id", Type: "int", IsPrimaryKey: true, IsAutoIncrement: true},
	}
	t.Columns=cols
	str,_:=json.Marshal(&t)
	log.Info().Msgf("%s",str)
	metaRepository:=meta.NewRepository(db)
	// Example ping request.
	r.POST("/mt", logger.SetLogger(
		logger.WithSkipPath([]string{"/skip"}),
		logger.WithUTC(true),
		logger.WithSkipPathRegexp(rxURL),
		logger.WithLogger(func(c *gin.Context, out io.Writer, latency time.Duration) zerolog.Logger {
			return zerolog.New(out).With().
				Str("foo", "bar").
				Str("path", c.Request.URL.Path).
				Dur("latency", latency).
				Logger()
		}),
	), func(c *gin.Context) {
		var metaTable meta.MetaTable
		if err := c.ShouldBindJSON(&metaTable); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		metaRepository.CreateMetaTableIfNotExists(&metaTable)
		r.GET(fmt.Sprintf("/%s",metaTable.TableName), func(c *gin.Context) {
			//fmt.Println(table.TableName)
			data,err:=metaRepository.SelectAll(&metaTable)
			if err!=nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, data)
		})
		c.String(http.StatusOK, "pong "+fmt.Sprint(time.Now().Unix()))
	})
	r.GET("/mt",func(c *gin.Context) {
		tables,err:=metaRepository.SelectMetaTables()
		if err!=nil{
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK,tables)
	})
	r.GET("/mt/:id",func(c *gin.Context) {
		tableId,err:=strconv.Atoi(c.Param("id"))
		if err!=nil{
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		table,err:=metaRepository.SelectMetaTableById(tableId)
		if err!=nil{
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK,table)
	})
	r.POST("/ct/:id",func(c *gin.Context) {
		tableId,err:=strconv.Atoi(c.Param("id"))
		if err!=nil{
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		table,err:=metaRepository.SelectMetaTableById(tableId)
		if err!=nil{
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		err=metaRepository.CreateTable(table)
		if err!=nil{
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK,table)
	})
	if tables,err:=metaRepository.SelectMetaTables();err==nil {
		for _,table:=range tables{
			r.GET(fmt.Sprintf("/%s",table.TableName), func(c *gin.Context) {
				//fmt.Println(table.TableName)
				data,err:=metaRepository.SelectAll(table)
				if err!=nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, data)
			})
		}
	}
	r.Run() // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")
}