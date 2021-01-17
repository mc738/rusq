use std::thread::JoinHandle;
use std::sync::mpsc::{Sender, Receiver, SendError};
use std::thread;
use rusqlite::{ToSql, Connection, NO_PARAMS, DatabaseName, Row, MappedRows};
use std::sync::mpsc;
use rlog::{Logger, Log};
use std::error::Error;
use std::path::Path;
use crate::common::{Query, Queryable, Value, BoxedValue, BlobValue, Criteria, ValueType, BlobRef, Transaction};

pub mod common;
pub mod queries;


pub enum WriteRequest {
    Query(Query),
    Transaction(Transaction)
}


/// A `rusq` context. 
pub struct Context {
    connection_string: String,
    db_writer: DbWriter,
    log: Log,
}

// A `DbWriter` is responsible for being the one writer source to the `sqlite` database.
// It receives `Queries` from `DataWriters` and executes them.
pub struct DbWriter {
    handler: JoinHandle<()>,
    sender: Sender<WriteRequest>,
}

pub struct DataWriter {
    sender: Sender<WriteRequest>
}

pub struct DataReader {
    connection: Connection,
    logger: Logger,
}

impl Context {
    pub fn create(connection_string: String) -> Result<Context, &'static str> {
        let log = Log::create()?;
        let connection = Context::create_connection(&connection_string)?;

        let db_writer = DbWriter::create(connection, log.get_logger())?;

        Ok(Context {
            connection_string,
            db_writer,
            log,
        })
    }

    pub fn get_connection(&self) -> Result<Connection, &'static str> {
        Context::create_connection(&self.connection_string)
    }


    pub fn get_writer(&self) -> Result<DataWriter, &'static str> {
        DataWriter::create(self.db_writer.sender.clone())
    }

    pub fn get_reader(&self) -> Result<DataReader, &'static str> {
        let connection = self.get_connection()?;
        let logger = self.log.get_logger();
        DataReader::create(connection, logger)
    }

    fn create_connection(connection_string: &String) -> Result<Connection, &'static str> {
        match rusqlite::Connection::open(connection_string) {
            Ok(connection) => Ok(connection),
            Err(_) => Err("Could not create connection")
        }
    }
}

impl DbWriter {
    pub(crate) fn create(mut conn: Connection, logger: Logger) -> Result<DbWriter, &'static str> {
        logger.log_info(String::from("db_writer"), format!("Starting..."));

        let (sender, receiver): (Sender<WriteRequest>, Receiver<WriteRequest>) = mpsc::channel();

        logger.log_success(String::from("db_writer"), format!("Started successfully"));
        let handler = thread::spawn(move || loop {
            let query = receiver.recv().unwrap();

            match query {
                WriteRequest::Query(query) => {
                    logger.log_info(String::from("db_writer"), format!("Query received, type: `{}`", query.get_type_name()));
                    logger.log_debug(String::from("db_writer"), format!("Sql: `{}`", query.get_raw_sql()));

                    match query.execute(&conn) {
                        Ok(_) => {
                            logger.log_success(String::from("db_writer"), format!("Query executed successfully."));
                        }
                        Err(e) => {
                            logger.log_error(String::from("db_writer"), format!("Could not execute query, error: `{}`", e));
                        }
                    }
                }
                WriteRequest::Transaction(transaction) => {
                    logger.log_info(String::from("db_writer"), String::from("Transaction received"));

                    let tx = conn.transaction().unwrap();

                    for query in transaction {
                        logger.log_debug(String::from("db_writer"), format!("Type: `{}`", query.get_type_name()));
                        logger.log_debug(String::from("db_writer"), format!("Sql: `{}`", query.get_raw_sql()));
                        match query.execute(&tx) {
                            Ok(_) => {
                                logger.log_success(String::from("db_writer"), format!("Query executed successfully."));
                            }
                            Err(e) => {
                                logger.log_error(String::from("db_writer"), format!("Could not execute query, error: `{}`", e));
                            }
                        }
                    }

                    tx.commit();
                }
            }
        });

        Ok(DbWriter {
            handler,
            sender,
        })
    }

    pub fn get_sender(&self) -> Sender<WriteRequest> {
        self.sender.clone()
    }
}

impl DataWriter {
    pub(crate) fn create(sender: Sender<WriteRequest>) -> Result<DataWriter, &'static str> {
        Ok(DataWriter {
            sender
        })
    }

    pub fn post(&self, query: WriteRequest) -> Result<(), &'static str> {
        match self.sender.send(query) {
            Ok(_) => Ok(()),
            Err(_) => Err("Could not send query. Check `db_writer` channel is not closed.")
        }
    }
}

impl DataReader {
    pub(crate) fn create(connection: Connection, logger: Logger) -> Result<DataReader, &'static str> {
        Ok(DataReader {
            connection,
            logger,
        })
    }

    pub fn get<T, F>(&self, table_name: &str, field_names: Vec<&str>, criteria: Option<Criteria>, mapper: F)
                     -> Result<Vec<T>, &'static str>
        where F: FnMut(&Row<'_>) -> Result<T, std::io::Error> {
        let fields = field_names.join(", ");

        let (sql, values) = match criteria {
            None => {
                // No params needs.
                let sql = format!("SELECT {} FROM {}", fields, table_name);

                (sql, None)
            }
            Some(c) => {
                let (crit_string, values) = Criteria::handle(c);


                let sql = format!("SELECT {} FROM {} WHERE {}", fields, table_name, crit_string);

                (sql, values)
            }
        };
        self.handle_get(sql, values, mapper)
    }

    fn handle_get<T, F>(&self, sql: String, params: Option<Vec<BoxedValue>>, mapper: F) -> Result<Vec<T>, &'static str> where F: FnMut(&Row<'_>) -> Result<T, std::io::Error> {

        self.logger.log_debug(String::from("db_reader"), format!("Sql: {}", sql));
        match params {
            None => DataReader::query_no_params(&self.connection, sql, mapper),
            Some(p) => DataReader::query_with_params(&self.connection, sql, p, mapper)
        }
    }

    pub fn query_no_params<T, F>(
        connection: &Connection,
        sql: String,
        mut f: F,
    ) -> Result<Vec<T>, &'static str> where
    //P: IntoIterator,
    //P::Item: ToSql,
        F: FnMut(&Row<'_>) -> Result<T, std::io::Error>, {
        let mut stmt = connection.prepare(sql.as_str()).unwrap();
        let mut result: Vec<T> = Vec::new();


        let rows = stmt.query_map(NO_PARAMS, |row| { Ok(f(row).unwrap()) }).unwrap();
        //let r = stmt.query_map(NO_PARAMS, |row| { Ok(f(row).unwrap()) }).unwrap();

        for row in rows {
            result.push(row.unwrap());
        }

        Ok(result)
    }

    pub fn query_with_params<T, F>(
        connection: &Connection,
        sql: String,
        params: Vec<BoxedValue>,
        mut f: F,
    ) -> Result<Vec<T>, &'static str> where
    //P: IntoIterator,
    //P::Item: ToSql,
        F: FnMut(&Row<'_>) -> Result<T, std::io::Error>, {
        let mut stmt = connection.prepare(sql.as_str()).unwrap();
        let mut result: Vec<T> = Vec::new();


        let rows = stmt.query_map(params, |row| { Ok(f(row).unwrap()) }).unwrap();

        //let r = stmt.query_map(NO_PARAMS, |row| { Ok(f(row).unwrap()) }).unwrap();

        for row in rows {
            result.push(row.unwrap());
        }

        Ok(result)
    }
}
