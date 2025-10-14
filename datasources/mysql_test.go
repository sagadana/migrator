package datasources

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/sagadana/migrator/helpers"
	"gorm.io/gorm"
)

func TestMySQLDatasource(t *testing.T) {
	t.Parallel()

	instanceId := helpers.RandomString(6)
	ctx := context.Background()

	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		mysqlDSN = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			os.Getenv("MYSQL_USER"),
			os.Getenv("MYSQL_PASS"),
			os.Getenv("MYSQL_HOST"),
			os.Getenv("MYSQL_PORT"),
			os.Getenv("MYSQL_DB"),
		)
	}
	mariaDBDSN := os.Getenv("MARIADB_DSN")
	if mariaDBDSN == "" {
		mariaDBDSN = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			os.Getenv("MYSQL_USER"),
			os.Getenv("MYSQL_PASS"),
			os.Getenv("MARIADB_HOST"),
			os.Getenv("MARIADB_PORT"),
			os.Getenv("MYSQL_DB"),
		)
	}

	// ---------------------- Simple Model  -----------------------

	// Define test model for MySQL
	type TestSimpleMySQLModel struct {
		ID     string `gorm:"primaryKey;column:id" json:"id"`
		FieldA string `gorm:"column:fieldA" json:"fieldA"`
		FieldB string `gorm:"column:fieldB" json:"fieldB"`
	}

	id := "mysql-simple-datasource"
	td := TestDatasource{
		id:              id,
		withFilter:      false,
		withWatchFilter: false,
		withSort:        false,
		source: NewMySQLDatasource(
			&ctx,
			MySQLDatasourceConfigs[TestSimpleMySQLModel]{
				DSN:       mysqlDSN,
				TableName: fmt.Sprintf("%s-%s", id, instanceId),
				Model:     &TestSimpleMySQLModel{},
				IDField:   TestIDField,

				OnInit: func(db *gorm.DB) error {
					return nil
				},

				DisableReplication: false,
			},
		),
	}
	runDatasourceTest(ctx, t, td)

	id = "mariadb-simple-datasource"
	td = TestDatasource{
		id:              id,
		withFilter:      false,
		withWatchFilter: false,
		withSort:        false,
		source: NewMySQLDatasource(
			&ctx,
			MySQLDatasourceConfigs[TestSimpleMySQLModel]{
				DSN:       mariaDBDSN,
				TableName: fmt.Sprintf("%s-%s", id, instanceId),
				Model:     &TestSimpleMySQLModel{},
				IDField:   TestIDField,
				DBFlavor:  MySQLFlavorMariaDB,

				OnInit: func(db *gorm.DB) error {
					return nil
				},

				DisableReplication: false,
			},
		),
	}
	runDatasourceTest(ctx, t, td)

	// ---------------------- Complex Model -----------------------

	// Define test model for MySQL
	type TestComplexMySQLModel struct {
		ID        string    `gorm:"primaryKey;column:id" json:"id"`
		FieldA    string    `gorm:"column:fieldA" json:"fieldA"`
		FieldB    string    `gorm:"column:fieldB" json:"fieldB"`
		FilterOut bool      `gorm:"column:filterOut" json:"filterOut"`
		SortAsc   int       `gorm:"type:int;column:sortAsc" json:"sortAsc"`
		CreatedAt time.Time `gorm:"column:createdAt" json:"createdAt"`
		UpdatedAt time.Time `gorm:"column:updatedAt" json:"updatedAt"`
	}

	id = "mysql-complex-datasource"
	td = TestDatasource{
		id:              id,
		withFilter:      true,
		withWatchFilter: false,
		withSort:        true,
		source: NewMySQLDatasource(
			&ctx,
			MySQLDatasourceConfigs[TestComplexMySQLModel]{
				DSN:       mysqlDSN,
				TableName: fmt.Sprintf("%s-%s", id, instanceId),
				Model:     &TestComplexMySQLModel{},
				IDField:   TestIDField,
				Filter: MySQLDatasourceFilter{
					Query:  fmt.Sprintf("`%s` = ?", TestFilterOutField),
					Params: []any{false},
				},
				Sort: map[string]any{
					TestSortAscField: 1, // 1 = Ascending, -1 = Descending
				},

				WithTransformer: func(data map[string]any) (TestComplexMySQLModel, error) {
					s, _ := strconv.Atoi(fmt.Sprintf("%v", data[TestSortAscField]))
					return TestComplexMySQLModel{
						ID:        fmt.Sprintf("%v", data[TestIDField]),
						FieldA:    fmt.Sprintf("%v", data[TestAField]),
						FieldB:    fmt.Sprintf("%v", data[TestBField]),
						FilterOut: data[TestFilterOutField] == true,
						SortAsc:   s,
					}, nil
				},
				OnInit: func(db *gorm.DB) error {
					return nil
				},

				DisableReplication: false,
			},
		),
	}
	runDatasourceTest(ctx, t, td)
}
