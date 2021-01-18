
use std::path::Path;
use rusqlite::{Connection, ToSql};

pub trait Queryable {
    fn execute(&self, connection: &Connection) -> Result<(),&'static str>;

    fn get_type_name(&self) -> & 'static str;
    fn get_raw_sql(&self) -> &'_ str;
}

pub type Transaction = Vec<Box<dyn Queryable + Send>>;

pub type Query = Box<dyn Queryable + Send>;

pub type BoxedValue = Box<dyn ToSql + Send + 'static>;

pub struct Value {
    pub(crate) field: String,
    pub(crate) value: ValueType
}

pub struct BlobValue {
    pub(crate) table: String,
    pub(crate) field: String,
    pub(crate) data: Vec<u8>,
}

pub enum BlobRef {
    File(Box<Path>),
    Memory(Vec<u8>),
}

pub enum ValueType {
    BoxedValue(BoxedValue),
    Blob(BlobRef)
}

pub enum Criteria {
    Raw(String),
    Items(Vec<CriteriaItemType>)
}

pub enum CriteriaItemType {
    Item(CriteriaItem),
    Raw(String),
    Group { items: Vec<CriteriaItemType>, logic: Logic }
}

pub enum Logic {
    And,
    Or
}

pub struct CriteriaItem {
    field: String,
    operator: String,
    value: CriteriaItemValue
}

pub enum CriteriaItemValue {
    Raw(String),
    Value(BoxedValue)
}

impl BlobRef {

    /// A static method to deconstruct a blob reference and return the raw blob data.
    /// The reference is considered spent once this is called.
    pub fn get<T>(table: T, field: T, blob_ref: BlobRef) -> Result<BlobValue, & 'static str> where T : Into<String> {

        
        let data = match blob_ref {
            BlobRef::File(path) => unimplemented!(),
            BlobRef::Memory(data) => Ok(data)
        }?;
        
        Ok(BlobValue {
            table: table.into(),
            field: field.into(),
            data
        })

    }
}

impl Criteria {
    pub fn handle(criteria: Criteria) -> (String, Option<Vec<BoxedValue>>) {
        match criteria {
            Criteria::Raw(s) => (s, None),
            Criteria::Items(_) => unimplemented!()
        }
    }
}

impl Value {
    pub fn create<T>(field: T, value: impl ToSql + Send + 'static) -> Value where T : Into<String> {
        Value {
            field: field.into(),
            value: ValueType::BoxedValue(Box::new(value))
        }
    }

    pub fn create_blob<T>(field: T, value: BlobRef) -> Value where T : Into<String> {
        Value {
            field: field.into(),
            value: ValueType::Blob(value)
        }
    }
}