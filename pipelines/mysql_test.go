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

func Test_MySQL_X_Memory_Pipeline(t *testing.T) {
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

	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		mysqlDSN = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
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

	// Define test model for MySQL
	type TestMySQLModel struct {
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

	// Transformer to convert map to TestMySQLModel
	mysqlTransformer := func(data map[string]any) (TestMySQLModel, error) {
		model := TestMySQLModel{
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

	// --------------- 1. Memory to MySQL --------------- //
	id = "test-mem-to-mysql-pipeline"
	fromDs = datasources.NewMemoryDatasource(getDsName(id, "source"), IDField)
	toDs = datasources.NewMySQLDatasource(
		&ctx,
		datasources.MySQLDatasourceConfigs[TestMySQLModel]{
			DSN:                mysqlDSN,
			TableName:          getDsName(id, "destination"),
			Model:              &TestMySQLModel{},
			IDField:            IDField,
			WithTransformer:    mysqlTransformer,
			DisableReplication: true,
		},
	)
	tp = TestPipeline{
		id:          id,
		source:      fromDs,
		destination: toDs,
	}
	runPipelineTest(ctx, t, tp)

	// --------------- 2. MySQL to Memory --------------- //
	id = "test-mysql-to-mem-pipeline"
	fromDs = datasources.NewMySQLDatasource(
		&ctx,
		datasources.MySQLDatasourceConfigs[TestMySQLModel]{
			DSN:                mysqlDSN,
			TableName:          getDsName(id, "source"),
			Model:              &TestMySQLModel{},
			IDField:            IDField,
			Filter:             datasources.MySQLDatasourceFilter{},
			WithTransformer:    mysqlTransformer,
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

	// --------------- 3. Memory to MaraiDB --------------- //
	id = "test-mem-to-mariadb-pipeline"
	fromDs = datasources.NewMemoryDatasource(getDsName(id, "source"), IDField)
	toDs = datasources.NewMySQLDatasource(
		&ctx,
		datasources.MySQLDatasourceConfigs[TestMySQLModel]{
			DSN:                mariaDBDSN,
			TableName:          getDsName(id, "destination"),
			Model:              &TestMySQLModel{},
			IDField:            IDField,
			DBFlavor:           datasources.MySQLFlavorMariaDB,
			WithTransformer:    mysqlTransformer,
			DisableReplication: true,
		},
	)
	tp = TestPipeline{
		id:          id,
		source:      fromDs,
		destination: toDs,
	}
	runPipelineTest(ctx, t, tp)

	// --------------- 4. MaraiDB to Memory --------------- //
	id = "test-mariadb-to-mem-pipeline"
	fromDs = datasources.NewMySQLDatasource(
		&ctx,
		datasources.MySQLDatasourceConfigs[TestMySQLModel]{
			DSN:                mariaDBDSN,
			TableName:          getDsName(id, "source"),
			Model:              &TestMySQLModel{},
			IDField:            IDField,
			DBFlavor:           datasources.MySQLFlavorMariaDB,
			Filter:             datasources.MySQLDatasourceFilter{},
			WithTransformer:    mysqlTransformer,
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
