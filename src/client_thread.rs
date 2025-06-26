
use std::{collections::HashMap, io::Error as IoError, thread, time::Duration};
use falcotcp::Client as RawClient;
use std::sync::{Mutex,Arc};

use crate::{albastream::{CompiledAlba, Error, ErrorKind}, db_response::DBResponse, handler::{CommitBuilder, CreateContainerBuilder, CreateRowBuilder, DeleteContainerBuilder, DeleteRowBuilder, EditRowBuilder, RollbackBuilder, SearchBuilder}};
pub struct Client{
    connection : Arc<Mutex<RawClient>>,
}
impl Client {
    pub fn connect(host : &str, password : [u8;32]) -> Result<Client, IoError>{
        let c = Arc::new(Mutex::new(RawClient::new(host, password)?));
        let cb = c.clone();
        thread::spawn(async move ||{
            loop {
                thread::sleep(Duration::from_secs(15));
                let _ = cb.lock().unwrap().ping();
            }
        });
        Ok(Client{connection:c})
    }
    pub fn execute(&self,compiled_command : CompiledAlba) -> Result<DBResponse,Error>{
        let mut client = self.connection.lock().unwrap();
        let b = match client.message(compiled_command){
            Ok(a) => a,
            Err(e) => {
                return Err(Error::new(ErrorKind::InvalidInput, e.to_string().as_str()))
            }
        };
        if b[0] == 1u8{
            return Err(Error::new(ErrorKind::Other, String::from_utf8_lossy(&b[1..]).to_string().as_str()))
        }
        Ok(DBResponse::from_bytes(&b[1..])?)
    }
}
impl Client {
    /// This method creates a builder for creating a search in which can be compiled into `CompiledAlba` later.
    pub fn build_search() -> SearchBuilder{
        return SearchBuilder { container: Vec::new(), column_names: Vec::new(), conditions: (Vec::new(),Vec::new()) }
    }

    pub fn build_edit_row() -> EditRowBuilder{
        return EditRowBuilder{ container: String::new(), changes: HashMap::new(), conditions: (Vec::new(),Vec::new()) }
    }

    pub fn build_delete_row() -> DeleteRowBuilder{
        return DeleteRowBuilder{ container: String::new(), conditions: (Vec::new(),Vec::new()) }
    }

    pub fn build_delete_container() -> DeleteContainerBuilder{
        return DeleteContainerBuilder{ container: String::new() }
    }

    pub fn build_create_row() -> CreateRowBuilder{
        return CreateRowBuilder{ container: String::new(), value: HashMap::new() }
    }

    pub fn build_create_container() -> CreateContainerBuilder{
        return CreateContainerBuilder{ container: String::new(), headers: HashMap::new() }
    }

    pub fn build_commit() -> CommitBuilder{
        return CommitBuilder{ container: None}
    }

    pub fn build_rollback() -> RollbackBuilder{
        return RollbackBuilder{ container: None}
    }
}

/// This macro builds the search macro, you can use it as a more straightforward way when compared to the main API (`Handler::build_search`).
/// 
/// bsrh stands for "build_search"
#[macro_export]
macro_rules! bsrh {
    (
        $(containers: [$($container:expr),* $(,)?])?
        $(columns: [$($column:expr),* $(,)?])?
        $(conditions: [
            $($condition_col:expr, $op:expr, $val:expr $(=> $logic:expr)?),* $(,)?
        ])?
    ) => {{
        let mut builder = Handler::build_search();
        
        $($(
            builder.add_container($container);
        )*)?
        
        $($(
            builder.add_column_name($column.to_string());
        )*)?
        
        $(
            let mut first = true;
            $(
                let logic = bsrh!(@logic $($logic)?, first);
                builder.add_conditions(
                    ($condition_col.to_string(), $op, $val),
                    logic
                );
                first = false;
            )*
        )?
        
        builder
    }};
    
    (@logic and, $first:expr) => { true };
    (@logic or, $first:expr) => { false };
    (@logic $other:expr, $first:expr) => { true };
    (@logic, $first:expr) => { true };
}

/// This macro is a similar version of the "bsrh" macro, the only difference is that it compiles in the end.
#[macro_export]
macro_rules! srch {
    ($($tt:tt)*) => {
        bsrh!($($tt)*).finish()
    };
}

/// Create and returns a logic operator enum
/// ### Panics
/// The macro do not panic
/// ### Errors
/// There is no errors from the macro itself
#[macro_export]
macro_rules! lo {
    (0) => { LogicalOperator::Equal };
    (1) => { LogicalOperator::Diferent };
    (2) => { LogicalOperator::Higher };
    (3) => { LogicalOperator::Lower };
    (4) => { LogicalOperator::HigherEquality };
    (5) => { LogicalOperator::LowerEquality };
    (6) => { LogicalOperator::StringContains };
    (7) => { LogicalOperator::StringContainsInsensitive };
    (8) => { LogicalOperator::StringRegex };
    
    (eq) => { LogicalOperator::Equal };
    (ne) => { LogicalOperator::Diferent };
    (gt) => { LogicalOperator::Higher };
    (lt) => { LogicalOperator::Lower };
    (gte) => { LogicalOperator::HigherEquality };
    (lte) => { LogicalOperator::LowerEquality };
    (contains) => { LogicalOperator::StringContains };
    (icontains) => { LogicalOperator::StringContainsInsensitive };
    (regex) => { LogicalOperator::StringRegex };
    
    (=) => { LogicalOperator::Equal };
    (!=) => { LogicalOperator::Diferent };
    (>) => { LogicalOperator::Higher };
    (<) => { LogicalOperator::Lower };
    (>=) => { LogicalOperator::HigherEquality };
    (<=) => { LogicalOperator::LowerEquality };
}