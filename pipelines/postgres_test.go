package pipelines

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/helpers"
)

func Test_Postgres_X_Memory_Pipeline(t *testing.T) {
	t.Parallel()

	instanceId := helpers.RandomString(6)
	ctx := context.Background()

	// Create datasource name
	getDsName := func(id string, name string) string {
		return fmt.Sprintf("%s-%s-%s", id, instanceId, name)
	}

	var id string
	var fromDs datasources.Datasource
	var toDs datasources.Datasource
	var tp TestPipeline

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

	// Define test model for PostgreSQL
	type TestPostgresModel struct {
		Index      string    `gorm:"primaryKey;column:Index" json:"Index"`
		CustomerId string    `gorm:"column:CustomerId" json:"CustomerId"`
		FirstName  string    `gorm:"column:FirstName" json:"FirstName"`
		LastName   string    `gorm:"column:LastName" json:"LastName"`
		Company    string    `gorm:"column:Company" json:"Company"`
		City       string    `gorm:"column:City" json:"City"`
		Country    string    `gorm:"column:Country" json:"Country"`
		Phone1     string    `gorm:"column:Phone1" json:"Phone1"`
		Phone2     string    `gorm:"column:Phone2" json:"Phone2"`
		Email      string    `gorm:"column:Email" json:"Email"`
		SubDate    time.Time `gorm:"column:SubscriptionDate" json:"SubscriptionDate"`
		Website    string    `gorm:"column:Website" json:"Website"`
		Test_Cr    bool      `gorm:"column:Test_Cr" json:"Test_Cr"` // Use for testing continuous replication
		CreatedAt  time.Time `gorm:"column:CreatedAt" json:"CreatedAt"`
		UpdatedAt  time.Time `gorm:"column:UpdatedAt" json:"UpdatedAt"`
	}

	// Transformer to convert map to TestPostgresModel
	postgresTransformer := func(data map[string]any) (TestPostgresModel, error) {
		model := TestPostgresModel{
			Index:      fmt.Sprintf("%v", data[IDField]),
			CustomerId: fmt.Sprintf("%v", data["Customer Id"]),
			FirstName:  fmt.Sprintf("%v", data["First Name"]),
			LastName:   fmt.Sprintf("%v", data["Last Name"]),
			Company:    fmt.Sprintf("%v", data["Company"]),
			City:       fmt.Sprintf("%v", data["City"]),
			Country:    fmt.Sprintf("%v", data["Country"]),
			Phone1:     fmt.Sprintf("%v", data["Phone 1"]),
			Phone2:     fmt.Sprintf("%v", data["Phone 2"]),
			Email:      fmt.Sprintf("%v", data["Email"]),
			Website:    fmt.Sprintf("%v", data["Website"]),
			Test_Cr:    fmt.Sprintf("%v", data["Test_Cr"]) == "true",
		}

		if val, ok := data["Subscription Date"]; ok && val != nil {
			model.SubDate, _ = time.Parse("2006-01-02", fmt.Sprintf("%v", val))
		} else {
			model.SubDate = time.Now()
		}
		if val, ok := data["CreatedAt"]; ok && val != nil {
			model.CreatedAt, _ = time.Parse("2006-01-02", fmt.Sprintf("%v", val))
		}
		if val, ok := data["UpdatedAt"]; ok && val != nil {
			model.UpdatedAt, _ = time.Parse("2006-01-02", fmt.Sprintf("%v", val))
		} else {
			model.UpdatedAt = time.Now()
		}

		return model, nil
	}

	// --------------- 1. Memory to Postgres --------------- //
	id = "test-mem-to-postgres-pipeline"
	fromDs = datasources.NewMemoryDatasource(getDsName(id, "source"), IDField)
	toDs = datasources.NewPostgresDatasource(
		&ctx,
		datasources.PostgresDatasourceConfigs[TestPostgresModel]{
			DSN:                postgresDSN,
			TableName:          getDsName(id, "destination"),
			Model:              &TestPostgresModel{},
			IDField:            IDField,
			WithTransformer:    postgresTransformer,
			DisableReplication: true,
		},
	)
	tp = TestPipeline{
		id:          id,
		source:      fromDs,
		destination: toDs,
	}
	runPipelineTest(ctx, t, tp)

	// --------------- 2. Postgres to Memory --------------- //
	id = "test-postgres-to-mem-pipeline"
	fromDs = datasources.NewPostgresDatasource(
		&ctx,
		datasources.PostgresDatasourceConfigs[TestPostgresModel]{
			DSN:                postgresDSN,
			TableName:          getDsName(id, "source"),
			Model:              &TestPostgresModel{},
			IDField:            IDField,
			Filter:             datasources.PostgresDatasourceFilter{},
			Sort:               map[string]any{},
			WithTransformer:    postgresTransformer,
			DisableReplication: false,
		},
	)
	toDs = datasources.NewMemoryDatasource(getDsName(id, "destination"), IDField)
	tp = TestPipeline{
		id:          id,
		source:      fromDs,
		destination: toDs,
	}
	runPipelineTest(ctx, t, tp)
}
