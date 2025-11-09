library(md)
describe("cd()", {

    it("switches to a valid database", {
        con <- DBI::dbConnect(duckdb::duckdb(), dbdir="memory")
        expect_output(cd(con, database_name = "memory"), "Status:")
        DBI::dbDisconnect(con)
    })

    it("switches to a valid schema within a database", {
        con <- DBI::dbConnect(duckdb::duckdb(), dbdir="memory")
        DBI::dbExecute(con, "CREATE SCHEMA test_schema;")
        expect_silent(cd(con, database_name = "memory", schema_name = "test_schema"))
        DBI::dbDisconnect(con)
    })

    it("errors on invalid database", {
        con <- DBI::dbConnect(duckdb::duckdb(), dbdir=":memory:")
        expect_error(cd(con, database_name = "invalid_db"))
        DBI::dbDisconnect(con)
    })

    it("errors on invalid schema", {
        con <- DBI::dbConnect(duckdb::duckdb(), dbdir=":memory:")
        expect_error(cd(con, database_name = "main", schema_name = "invalid_schema"))
        DBI::dbDisconnect(con)
    })

})
