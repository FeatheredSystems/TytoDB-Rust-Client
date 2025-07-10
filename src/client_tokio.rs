use std::{io::Error as IoError, time::Duration};
use falcotcp::Client as RawClient;
use tokio::sync::Mutex;
use std::sync::Arc;

use crate::{albastream::{CompiledAlba, Error, ErrorKind}, db_response::DBResponse, handler::{CommitBuilder, CreateContainerBuilder, CreateRowBuilder, DeleteContainerBuilder, DeleteRowBuilder, EditRowBuilder, RollbackBuilder, SearchBuilder,BatchCreateRowsBuilder, BatchBuilder}};
pub struct Client{
    connection : Arc<Mutex<RawClient>>,
}
impl Client {
    pub async fn connect(host : &str, password : [u8;32]) -> Result<Client, IoError>{
        let c = Arc::new(Mutex::new(RawClient::new(host, password).await?));
        let cb = c.clone();
        
        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                let _ = cb.lock().await.ping();
            }
        });
        
        Ok(Client{connection:c})
    }
    pub async fn execute(&self,compiled_command : CompiledAlba) -> Result<DBResponse,Error>{
        let mut client = self.connection.lock().await;
        let b = match client.message(compiled_command).await{
            Ok(a) => a,
            Err(e) => {
                return Err(Error::new(ErrorKind::InvalidInput, e.to_string().as_str()))
            }
        };
        //println!("{:?}",b);
        if b[0] == 1u8{
            return Err(Error::new(ErrorKind::Other, String::from_utf8_lossy(&b[1..]).to_string().as_str()))
        }
        Ok(DBResponse::from_bytes(&b[1..=b.len()-1])?)
    }
}
impl Client {
    /// This method creates a builder for creating a search in which can be compiled into `CompiledAlba` later.
    pub fn build_search() -> SearchBuilder{
        return SearchBuilder { container: String::new(), column_names: Vec::new(), conditions: (Vec::new(),Vec::new()) }
    }

    pub fn build_edit_row() -> EditRowBuilder{
        return EditRowBuilder{ container: String::new(), changes: (Vec::new(),Vec::new()), conditions: (Vec::new(),Vec::new()) }
    }

    pub fn build_delete_row() -> DeleteRowBuilder{
        return DeleteRowBuilder{ container: String::new(), conditions: (Vec::new(),Vec::new()) }
    }

    pub fn build_delete_container() -> DeleteContainerBuilder{
        return DeleteContainerBuilder{ container: String::new() }
    }

    pub fn build_create_row() -> CreateRowBuilder{
        return CreateRowBuilder{ container: String::new(), value: (Vec::new(),Vec::new()) }
    }

    pub fn build_batch_create_row() -> BatchCreateRowsBuilder{
        return BatchCreateRowsBuilder{ container: String::new(), value: (Vec::new(),Vec::new()) }
    }

    pub fn build_create_container() -> CreateContainerBuilder{
        return CreateContainerBuilder{ container: String::new(), headers: (Vec::new(),Vec::new()) }
    }

    pub fn build_commit() -> CommitBuilder{
        return CommitBuilder{ container: None}
    }

    pub fn build_rollback() -> RollbackBuilder{
        return RollbackBuilder{ container: None}
    }
    pub fn build_batch() -> BatchBuilder{
        return BatchBuilder::new()
    }
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


    ("=") => { LogicalOperator::Equal };
    ("!=") => { LogicalOperator::Diferent };
    (">") => { LogicalOperator::Higher };
    ("<") => { LogicalOperator::Lower };
    (">=") => { LogicalOperator::HigherEquality };
    ("<=") => { LogicalOperator::LowerEquality };
    ("&>") => { LogicalOperator::StringContains };
    ("&&>") => { LogicalOperator::StringContainsInsensitive };
    ("&&&>") => { LogicalOperator::StringRegex };
    
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
