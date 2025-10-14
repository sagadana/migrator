package datasources

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sagadana/migrator/helpers"
	"gorm.io/gorm"
)

func TestPostgresDatasource(t *testing.T) {
	t.Parallel()

	instanceId := helpers.RandomString(6)
	ctx := context.Background()

	postgresDSN := os.Getenv("POSTGRES_DSN")
	if postgresDSN == "" {
		postgresDSN = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			os.Getenv("POSTGRES_HOST"),
			os.Getenv("POSTGRES_PORT"),
			os.Getenv("POSTGRES_USER"),
			os.Getenv("POSTGRES_PASS"),
			os.Getenv("POSTGRES_DB"),
			os.Getenv("POSTGRES_SSLMODE"),
		)
	}

	// ---------------------- Simple Model  -----------------------

	// Define test model for PostgreSQL
	type TestSimplePostgresModel struct {
		ID     string `gorm:"primaryKey;column:id" json:"id"`
		FieldA string `gorm:"column:fieldA" json:"fieldA"`
		FieldB string `gorm:"column:fieldB" json:"fieldB"`
	}

	id := "postgres-simple-datasource"
	td := TestDatasource{
		id:              id,
		withFilter:      false,
		withWatchFilter: false,
		withSort:        false,
		source: NewPostgresDatasource(
			&ctx,
			PostgresDatasourceConfigs[TestSimplePostgresModel]{
				DSN:       postgresDSN,
				TableName: fmt.Sprintf("%s-%s", id, instanceId),
				Model:     &TestSimplePostgresModel{},
				IDField:   TestIDField,

				OnInit: func(db *gorm.DB) error {
					return nil
				},

				DisableReplication: false,
			},
		),
	}
	runDatasourceTest(ctx, t, td)

	// ---------------------- Complex Model -----------------------

	// Define test model for PostgreSQL
	type TestComplexPostgresModel struct {
		ID        string    `gorm:"primaryKey;column:id" json:"id"`
		FieldA    string    `gorm:"column:fieldA" json:"fieldA"`
		FieldB    string    `gorm:"column:fieldB" json:"fieldB"`
		FilterOut bool      `gorm:"column:filterOut" json:"filterOut"`
		SortAsc   int       `gorm:"type:int;column:sortAsc" json:"sortAsc"`
		CreatedAt time.Time `gorm:"column:createdAt" json:"createdAt"`
		UpdatedAt time.Time `gorm:"column:updatedAt" json:"updatedAt"`
	}

	id = "postgres-complex-datasource"
	td = TestDatasource{
		id:              id,
		withFilter:      true,
		withWatchFilter: false,
		withSort:        true,
		source: NewPostgresDatasource(
			&ctx,
			PostgresDatasourceConfigs[TestComplexPostgresModel]{
				DSN:       postgresDSN,
				TableName: fmt.Sprintf("%s-%s", id, instanceId),
				Model:     &TestComplexPostgresModel{},
				IDField:   TestIDField,
				Filter: PostgresDatasourceFilter{
					Query:  fmt.Sprintf("\"%s\" = ?", TestFilterOutField),
					Params: []any{false},
				},
				Sort: map[string]any{
					TestSortAscField: 1, // 1 = Ascending, -1 = Descending
				},

				WithTransformer: func(data map[string]any) (TestComplexPostgresModel, error) {
					s, _ := strconv.Atoi(fmt.Sprintf("%v", data[TestSortAscField]))
					return TestComplexPostgresModel{
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

				PublicationName:     strings.ReplaceAll(fmt.Sprintf("%s_pub", id), "-", "_"),
				ReplicationSlotName: strings.ReplaceAll(fmt.Sprintf("%s_slot", id), "-", "_"),
				DisableReplication:  false,
			},
		),
	}
	runDatasourceTest(ctx, t, td)
}
