use rusqlite::{ToSql, Connection, DatabaseName, NO_PARAMS};
use crate::common::{BoxedValue, BlobValue, Value, Query, ValueType, BlobRef, Criteria, Queryable};

pub struct Generic {
    sql: String,
    values: Vec<BoxedValue>,
}

pub struct Insert {
    sql: String,
    values: Vec<BoxedValue>,
    blobs: Option<Vec<BlobValue>>,
}

pub struct Create {
    sql: String
}

pub struct Update {
    sql: String,
    values: Vec<BoxedValue>,
    blobs: Option<Vec<BlobValue>>,
}

pub struct Delete {
    sql: String,
    values: Option<Vec<BoxedValue>>,
}

pub struct UpdateBlob {
    sql: String,
    data: Vec<u8>,
    row_id: i64,
    table_name: String,
    field_name: String,
}

impl Generic {
    pub fn create(sql: String, values: Vec<impl ToSql + Send + 'static>) -> Result<Query, &'static str> {

        //let params = params! [ values ];

        let mut params: Vec<Box<dyn ToSql + Send + 'static>> = Vec::new();

        for v in values {
            params.push(Box::new(v));
        }

        Ok(Box::new(Generic {
            sql,
            values: params,
        }))
    }
}

impl Insert {
    pub fn create(table_name: String, values: Vec<Value>) -> Result<Query, &'static str> {
        let (fields, params_string, values, blobs) = Insert::handle_values(table_name.clone(), values)?;
        let sql = format!("INSERT INTO {} ({}) VALUES ({});", table_name, fields, params_string);


        Ok(Box::new(Insert {
            sql,
            values,
            blobs,
        }))
    }

    fn handle_values(table_name: String, values: Vec<Value>) -> Result<(String, String, Vec<BoxedValue>, Option<Vec<BlobValue>>), &'static str> {
        let mut fields = Vec::new();
        let mut params_string = Vec::new();
        let mut result_values: Vec<BoxedValue> = Vec::new();
        let mut blobs: Vec<BlobValue> = Vec::new();

        let mut counter: u8 = 1;

        for value in values {
            println!("TEST");

            fields.push(value.field.clone());

            match value.value {
                ValueType::BoxedValue(boxed) => {
                    println!("TEST");
                    params_string.push(format!("?{}", counter));
                    result_values.push(boxed);
                    counter = counter + 1;
                }
                ValueType::Blob(blob) => {
                    println!("{}", value.field);
                    let loaded_blob = BlobRef::get(table_name.clone(), value.field.clone(), blob)?;
                    params_string.push(format!("ZEROBLOB({})", loaded_blob.data.len()));
                    blobs.push(loaded_blob);
                }
            }
        };

        Ok((fields.join(", "), params_string.join(", "), result_values, vec_to_optional(blobs)))
    }
}

impl Create {
    pub fn create(sql: String) -> Result<Query, &'static str> {
        Ok(Box::new(Create {
            sql
        }))
    }
}

impl Update {
    pub fn create(table_name: String, values: Vec<Value>, criteria: Criteria) -> Result<Query, &'static str> {
        let (params_string, mut values, blobs) = Update::handle_values(table_name.clone(), values)?;
        let (criteria_string, params) = Criteria::handle(criteria);

        let sql = format!("UPDATE {} SET {} WHERE {};", table_name, params_string, criteria_string);

        match params {
            None => {}
            Some(mut p) => values.append(&mut p)
        }

        Ok(Box::new(Update {
            sql,
            values,
            blobs,
        }))
    }


    pub fn handle_values(table_name: String, values: Vec<Value>) -> Result<(String, Vec<BoxedValue>, Option<Vec<BlobValue>>), &'static str> {
        //let mut fields = Vec::new();
        let mut params_string = Vec::new();
        let mut result_values: Vec<BoxedValue> = Vec::new();
        let mut blobs: Vec<BlobValue> = Vec::new();
        let mut counter: u8 = 1;


        for value in values {

            //fields.push(value.field.clone());

            match value.value {
                ValueType::BoxedValue(boxed) => {
                    //fields.push(value.field);
                    params_string.push(format!("{} = ?{}", value.field, counter));
                    result_values.push(boxed);
                    counter = counter + 1;
                }
                ValueType::Blob(blob) => {
                    let loaded_blob = BlobRef::get(table_name.clone(), value.field.clone(), blob)?;
                    params_string.push(format!("{} = ZEROBLOB({})", value.field, loaded_blob.data.len()));
                    blobs.push(loaded_blob);
                }
            }
        };

        Ok((params_string.join(", "), result_values, vec_to_optional(blobs)))
    }
}

impl Delete {
    pub fn create(table_name: String, criteria: Criteria) -> Result<Query, &'static str> {
        let (criteria_string, params) = Criteria::handle(criteria);

        let sql = format!("DELETE FROM {} WHERE {};", table_name, criteria_string);


        Ok(Box::new(Delete {
            sql,
            values: params,
        }))
    }
}

impl UpdateBlob {
    pub fn create(table_name: String, field_name: String, row_id: i64, data: Vec<u8>) -> Result<Query, &'static str> {
        let sql = format!("UPDATE {} SET {} = ZEROBLOB({}) WHERE rowid = {};", table_name, field_name, data.len(), row_id);

        Ok(Box::new(UpdateBlob {
            sql,
            data,
            row_id,
            table_name,
            field_name
        }))
    }
}

impl Queryable for Generic {
    fn execute(&self, connection: &Connection) -> Result<(), &'static str> {
        match connection.execute(&self.sql, &self.values) {
            Ok(_) => Ok(()),
            Err(err) => {
                println!("Err: {:?}", err);
                Err("Could not execute `INSERT`. Table might not exist, there is an issue with the query or the database is unavailable.")
            }
        }
    }

    fn get_type_name(&self) -> &'static str {
        "GENERIC"
    }

    fn get_raw_sql(&self) -> &'_ str {
        self.sql.as_str()
    }
}

impl Queryable for Insert {
    fn execute(&self, connection: &Connection) -> Result<(), &'static str> {
        match connection.execute(&self.sql, &self.values) {
            Ok(_) => {
                match &self.blobs {
                    None => Ok(()),
                    Some(blobs) => {
                        let row_id = connection.last_insert_rowid();

                        for blob in blobs {
                            let mut b = connection.blob_open(DatabaseName::Main, blob.table.as_str(), blob.field.as_str(), row_id, false).unwrap();
                            b.write_at(blob.data.as_slice(), 0);
                        }

                        Ok(())
                    }
                }
            }
            Err(err) => {
                println!("Err: {:?}", err);
                Err("Could not execute `INSERT`. Table might not exist, there is an issue with the query or the database is unavailable.")
            }
        }
    }

    fn get_type_name(&self) -> &'static str {
        "INSERT"
    }

    fn get_raw_sql(&self) -> &'_ str {
        self.sql.as_str()
    }
}

impl Queryable for Create {
    fn execute(&self, connection: &Connection) -> Result<(), &'static str> {
        match connection.execute(&self.sql, NO_PARAMS) {
            Ok(_) => Ok(()),
            Err(err) => {
                // TODO log error details somewhere.
                // println!("Err: {:?}", err);
                Err("Could not execute `CREATE`. Table might already exist or the database is unavailable.")
            }
        }
    }

    fn get_type_name(&self) -> &'static str {
        "CREATE"
    }

    fn get_raw_sql(&self) -> &'_ str {
        self.sql.as_str()
    }
}

impl Queryable for Update {
    fn execute(&self, connection: &Connection) -> Result<(), &'static str> {
        match connection.execute(&self.sql, &self.values) {
            Ok(_) => {
                match &self.blobs {
                    None => Ok(()),
                    Some(blobs) => {
                        let row_id = connection.last_insert_rowid();

                        for blob in blobs {
                            let mut b = connection.blob_open(DatabaseName::Main, blob.table.as_str(), blob.field.as_str(), row_id, false).unwrap();
                            b.write_at(blob.data.as_slice(), 0);
                        }

                        Ok(())
                    }
                }
            }
            Err(err) => {
                println!("Err: {:?}", err);
                Err("Could not execute `UPDATE`. Table might not exist, there is an issue with the query or the database is unavailable.")
            }
        }
    }

    fn get_type_name(&self) -> &'static str {
        "UPDATE"
    }

    fn get_raw_sql(&self) -> &str {
        self.sql.as_str()
    }
}

impl Queryable for Delete {
    fn execute(&self, connection: &Connection) -> Result<(), &'static str> {
        match &self.values {
            None => {
                match connection.execute(&self.sql, NO_PARAMS) {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        println!("Err: {:?}", err);
                        Err("Could not execute `DELETE`. Table might not exist, there is an issue with the query or the database is unavailable.")
                    }
                }
            }
            Some(p) => {
                match connection.execute(&self.sql, p) {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        println!("Err: {:?}", err);
                        Err("Could not execute `DELETE`. Table might not exist, there is an issue with the query or the database is unavailable.")
                    }
                }
            }
        }
    }

    fn get_type_name(&self) -> &'static str {
        "DELETE"
    }

    fn get_raw_sql(&self) -> &str {
        self.sql.as_str()
    }
}

impl Queryable for UpdateBlob {
    fn execute(&self, connection: &Connection) -> Result<(), &'static str> {
        match connection.execute(&self.sql, NO_PARAMS) {
            Ok(_) => {
                let row_id = self.row_id;

                let mut b = connection.blob_open(DatabaseName::Main, self.table_name.as_str(), self.field_name.as_str(), self.row_id, false).unwrap();
                b.write_at(self.data.as_slice(), 0);

                Ok(())
            }
            Err(err) => {
                println!("Err: {:?}", err);
                Err("Could not execute `UPDATE`. Table might not exist, there is an issue with the query or the database is unavailable.")
            }
        }
    }

    fn get_type_name(&self) -> &'static str {
        "UPDATE_BLOB"
    }

    fn get_raw_sql(&self) -> &str {
        self.sql.as_str()
    }
}

fn vec_to_optional<T>(vec: Vec<T>) -> Option<Vec<T>> {
    match vec.is_empty() {
        true => None,
        false => Some(vec)
    }
}