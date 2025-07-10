

use crate::{albastream::{CompiledAlba, Error}, commands::{AlbaContainer, Batch, BatchCreateRows, Commands, Commit, CreateContainer, CreateRow, DeleteContainer, DeleteRow, Rollback}, logical_operators::LogicalOperator, types::AlbaTypes};
use crate::commands::Search;
use crate::commands::EditRow;

/// API for building a `Search` structure
pub struct SearchBuilder{
    pub container: AlbaContainer,
    pub column_names: Vec<String>,
    pub conditions: (Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(u8,char)>)
}

pub trait BatchingItem{
    fn into_batching_item(self) -> Commands;
}

impl SearchBuilder {
    pub fn new() -> Self {
        Self {
            container: String::new(),
            column_names: Vec::new(),
            conditions: (Vec::new(), Vec::new())
        }
    }

    /// Add a new container to the `Search` structure being built.
    pub fn add_container(mut self, container: AlbaContainer)-> Self{
        self.container = container;
        self
    }
    /// Add a new column_name to the the `Search` structure being built.
    pub fn add_column_name(mut self, column: String)-> Self{
        self.column_names.push(column);
        self
    }
    /// Add a new condition to the condition chain of the `Search` structure being built.
    /// logic -> true = AND, false = OR 
    pub fn add_conditions(mut self, condition: (String,LogicalOperator,AlbaTypes), logic: bool)-> Self{
        self.conditions.0.push(condition);
        let l = self.conditions.0.len() as u8;
        if l == 1 {
            self.conditions.1.push((0, 'a')); 
        } else {
            self.conditions.1.push((l - 1, if logic{'a'}else{'o'}));
        }
        self
    }
    /// Finish the builder, returning the compiled `Search` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::Search(Search{
            container: self.container,
            conditions: self.conditions,
            col_nam: self.column_names
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `Search` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::Search(Search{
            container: self.container.clone(),
            conditions: self.conditions.clone(),
            col_nam: self.column_names.clone()
        }).compile()? as CompiledAlba)
    }
}

pub struct EditRowBuilder{
    pub (crate) container : String,
    pub (crate) changes : (Vec<String>,Vec<AlbaTypes>),
    pub (crate) conditions : (Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(u8,char)>)
}
impl EditRowBuilder {
    pub fn new() -> Self {
        Self {
            container: String::new(),
            changes: (Vec::new(), Vec::new()),
            conditions: (Vec::new(), Vec::new())
        }
    }
    /// Set the container to the `EditRow` structure being built.
    pub fn put_container(mut self, container: String)-> Self{
        self.container = container;
        self
    }
    /// Set a change to the change list of the `EditRow` structure being built.
    pub fn edit_column(mut self, column: String,new_value : AlbaTypes)-> Self{
        self.changes.0.push(column);
        self.changes.1.push(new_value);
        self
    }
    /// Add a new condition to the condition chain of the `EditRow` structure being built.
    /// logic -> true = AND, false = OR 
    pub fn add_conditions(mut self, condition: (String,LogicalOperator,AlbaTypes), logic: bool)-> Self{
        self.conditions.0.push(condition);
        let l = self.conditions.0.len() as u8;
        if l > 1{
            self.conditions.1.push((l,if logic{'a'}else{'o'}));
        }
        self
    }
    /// Finish the builder, returning the compiled `Search` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        let col_nam : Vec<String> = self.changes.0;
        let col_val : Vec<AlbaTypes> = self.changes.1;
        Ok(Commands::EditRow(EditRow{
            container: self.container,
            conditions: self.conditions,
            col_nam,
            col_val
            
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `Search` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        let col_nam : Vec<String> = self.changes.0.clone();
        let col_val : Vec<AlbaTypes> = self.changes.1.clone();
        Ok(Commands::EditRow(EditRow{
            container: self.container.clone(),
            conditions: self.conditions.clone(),
            col_nam,
            col_val
            
        }).compile()? as CompiledAlba)
    }
}



pub struct DeleteRowBuilder{
    pub(crate) container : String,
    pub(crate) conditions : (Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(usize,char)>)
}
impl DeleteRowBuilder {
    pub fn new() -> Self {
        Self {
            container: String::new(),
            conditions: (Vec::new(), Vec::new())
        }
    }

    /// Set the container to the `DeleteRow` structure being built.
    pub fn put_container(mut self, container: String)-> Self{
        self.container = container;
        self
    }
    /// Add a new condition to the condition chain of the `DeleteRow` structure being built.
    /// logic -> true = AND, false = OR 
    pub fn add_conditions(mut self, condition: (String,LogicalOperator,AlbaTypes), logic: bool)-> Self{
        self.conditions.0.push(condition);
        let l = self.conditions.0.len();
        if l > 1{
            self.conditions.1.push((l,if logic{'a'}else{'o'}));
        }
        self
    }
    /// Finish the builder, returning the compiled `DeleteRow` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::DeleteRow(DeleteRow{
            container: self.container,
            conditions: Some(self.conditions)
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `DeleteRow` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::DeleteRow(DeleteRow{
            container: self.container.clone(),
            conditions: Some(self.conditions.clone())
            
        }).compile()? as CompiledAlba)
    }
}


pub struct DeleteContainerBuilder{
    pub(crate) container : String
}
impl DeleteContainerBuilder {
    pub fn new() -> Self {
        Self {
            container: String::new()
        }
    }

    /// Set the container to the `DeleteContainer` structure being built.
    pub fn put_container(mut self, container: String)-> Self{
        self.container = container;
        self
    }
    /// Finish the builder, returning the compiled `DeleteContainer` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::DeleteContainer(DeleteContainer{
            container: self.container,
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `DeleteContainer` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::DeleteContainer(DeleteContainer{
            container: self.container.clone()
            
        }).compile()? as CompiledAlba)
    }
}


pub struct CreateRowBuilder{
    pub(crate) container : String,
    pub(crate) value : (Vec<String>,Vec<AlbaTypes>)
}
impl CreateRowBuilder {
    pub fn new() -> Self {
        Self {
            container: String::new(),
            value: (Vec::new(), Vec::new())
        }
    }
    /// Set the container to the `CreateRow` structure being built.
    pub fn put_container(mut self, container: String)-> Self{
        self.container = container.to_string();
        self
    }

    pub fn insert_value(mut self,column : String,value: AlbaTypes)-> Self{
        self.value.0.push(column);
        self.value.1.push(value);
        self
    }
    /// Finish the builder, returning the compiled `CreateRow` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::CreateRow(CreateRow{
            container: self.container,
            col_nam: self.value.0,
            col_val: self.value.1
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `CreateRow` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::CreateRow(CreateRow{
            container: self.container.clone(),
            col_nam: self.value.0.clone(),
            col_val: self.value.1.clone()
        }).compile()? as CompiledAlba)
    }
}



pub struct CreateContainerBuilder{
    pub container : String,
    pub(crate) headers : (Vec<String>,Vec<u8>)
}
impl CreateContainerBuilder {
    pub fn new() -> Self {
        Self {
            container: String::new(),
            headers: (Vec::new(), Vec::new())
        }
    }
    /// Set the container to the `CreateContainer` structure being built.
    pub fn put_container(mut self, container: String)-> Self{
        self.container = container;
        self
    }

    /// Insert the metadata to creating a new container
    pub fn insert_header(mut self,column_name : String,column_type: u8)-> Self{
        self.headers.0.push(column_name);
        self.headers.1.push(column_type);
        self
    }
    /// Finish the builder, returning the compiled `CreateContainer` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::CreateContainer(CreateContainer{
            name: self.container,
            col_nam: self.headers.0,
            col_val: self.headers.1
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `CreateContainer` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::CreateContainer(CreateContainer{
            name: self.container.clone(),
            col_nam: self.headers.0.clone(),
            col_val: self.headers.1.clone()
        }).compile()? as CompiledAlba)
    }
}


pub struct CommitBuilder{
    pub(crate) container : Option<String>,
}
impl CommitBuilder {
    pub fn new() -> Self {
        Self {
            container: None
        }
    }
    /// Set the container you're commiting to
    pub fn set_container(mut self,container : String)-> Self{
        self.container = Some(container);
        self
    }
    /// Finish the builder, returning the compiled `Commit` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::Commit(Commit{
            container: self.container,
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `Commit` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::Commit(Commit{
            container: self.container.clone(),
        }).compile()? as CompiledAlba)
    }
}

pub struct RollbackBuilder{
    pub(crate) container : Option<String>,
}
impl RollbackBuilder {
    pub fn new() -> Self {
        Self {
            container: None
        }
    }
    /// Set the container to the `Rollback` structure being built.
    pub fn put_container(mut self, container: String)-> Self{
        self.container = Some(container);
        self
    }

    /// Set the container you're rolling back
    pub fn set_container(mut self,container : String)-> Self{
        self.container = Some(container);
        self
    }
    /// Finish the builder, returning the compiled `Rollback` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::Rollback(Rollback{
            container: self.container,
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `Rollback` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::Rollback(Rollback{
            container: self.container.clone(),
        }).compile()? as CompiledAlba)
    }
}


pub struct BatchCreateRowsBuilder{
    pub(crate) container : String,
    pub(crate) value : (Vec<String>,Vec<Vec<AlbaTypes>>)
}
impl BatchCreateRowsBuilder {
    pub fn new() -> Self {
        Self {
            container: String::new(),
            value: (Vec::new(), Vec::new())
        }
    }
    /// Set the container to the `BatchCreateRow` structure being built.
    pub fn put_container(mut self, container: String)-> Self{
        self.container = container.to_string();
        self
    }
    pub fn set_columns(mut self, columns : Vec<String>) -> Self{
        self.value.0 = columns;
        self
    }
    pub fn insert_value(mut self,value: Vec<AlbaTypes>)-> Self{
        self.value.1.push(value);
        self
    }
    /// Finish the builder, returning the compiled `BatchCreateRow` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::BatchCreateRows(BatchCreateRows{
            container: self.container,
            col_nam: self.value.0,
            col_val: self.value.1
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `BatchCreateRow` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::BatchCreateRows(BatchCreateRows{
            container: self.container.clone(),
            col_nam: self.value.0.clone(),
            col_val: self.value.1.clone()
        }).compile()? as CompiledAlba)
    }
}


#[derive(Debug,Clone)]
pub struct BatchBuilder{
    pub(crate) transaction : bool,
    pub(crate) value : Vec<Commands> 
}
impl BatchBuilder {
    pub fn new() -> Self {
        Self {
            transaction:false,
            value : Vec::new()
        }
    }

    /// Define whether the current batching is or not a transaction batching.
    fn transaction(mut self,bool:bool) -> Self{
        self.transaction = bool;
        self
    }

    /// Insert a operation into the batching
    fn push<VERYNICEITEM:BatchingItem>(mut self, bin : VERYNICEITEM) -> Self{
        self.value.push(bin.into_batching_item());
        self
    }
    
    fn finish(self) -> Result<CompiledAlba,Error>{
        self.into_batching_item().compile()
    }
    fn cloned_finish(self) -> Result<CompiledAlba,Error>{
        self.clone().into_batching_item().compile()
    }
}
impl BatchingItem for BatchBuilder{
    fn into_batching_item(self) -> Commands {
        Commands::Batch(Batch{
            transaction: self.transaction,
            commands: self.value
        })  
    }
}
impl BatchingItem for Commands{
    fn into_batching_item(self) -> Commands {
        self
    }
}
impl BatchingItem for SearchBuilder {
    fn into_batching_item(self) -> Commands {
        Commands::Search(Search{
            container: self.container,
            conditions: self.conditions,
            col_nam: self.column_names
        })
    }
}

impl BatchingItem for EditRowBuilder {
    fn into_batching_item(self) -> Commands {
        let col_nam : Vec<String> = self.changes.0;
        let col_val : Vec<AlbaTypes> = self.changes.1;
        Commands::EditRow(EditRow{
            container: self.container,
            conditions: self.conditions,
            col_nam,
            col_val
        })
    }
}

impl BatchingItem for DeleteRowBuilder {
    fn into_batching_item(self) -> Commands {
        Commands::DeleteRow(DeleteRow{
            container: self.container,
            conditions: Some(self.conditions)
        })
    }
}

impl BatchingItem for DeleteContainerBuilder {
    fn into_batching_item(self) -> Commands {
        Commands::DeleteContainer(DeleteContainer{
            container: self.container,
        })
    }
}

impl BatchingItem for CreateRowBuilder {
    fn into_batching_item(self) -> Commands {
        Commands::CreateRow(CreateRow{
            container: self.container,
            col_nam: self.value.0,
            col_val: self.value.1
        })
    }
}

impl BatchingItem for CreateContainerBuilder {
    fn into_batching_item(self) -> Commands {
        Commands::CreateContainer(CreateContainer{
            name: self.container,
            col_nam: self.headers.0,
            col_val: self.headers.1
        })
    }
}

impl BatchingItem for CommitBuilder {
    fn into_batching_item(self) -> Commands {
        Commands::Commit(Commit{
            container: self.container,
        })
    }
}

impl BatchingItem for RollbackBuilder {
    fn into_batching_item(self) -> Commands {
        Commands::Rollback(Rollback{
            container: self.container,
        })
    }
}

impl BatchingItem for BatchCreateRowsBuilder {
    fn into_batching_item(self) -> Commands {
        Commands::BatchCreateRows(BatchCreateRows{
            container: self.container,
            col_nam: self.value.0,
            col_val: self.value.1
        })
    }
}
