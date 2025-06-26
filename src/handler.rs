use std::collections::HashMap;

use crate::{albastream::{CompiledAlba, Error}, commands::{AlbaContainer, Commands, Commit, CreateContainer, CreateRow, DeleteContainer, DeleteRow}, logical_operators::LogicalOperator, types::AlbaTypes};
use crate::commands::Search;
use crate::commands::EditRow;

/// API for building a `Search` structure
pub struct SearchBuilder{
    pub container: Vec<AlbaContainer>,
    pub column_names: Vec<String>,
    pub conditions: (Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(usize,char)>)
}

impl SearchBuilder {
    /// Add a new container to the `Search` structure being built.
    pub fn add_container(&mut self, container: AlbaContainer){
        self.container.push(container);
    }
    /// Add a new column_name to the the `Search` structure being built.
    pub fn add_column_name(&mut self, column: String){
        self.column_names.push(column);
    }
    /// Add a new condition to the condition chain of the `Search` structure being built.
    /// logic -> true = AND, false = OR 
    pub fn add_conditions(&mut self, condition: (String,LogicalOperator,AlbaTypes), logic: bool){
        self.conditions.0.push(condition);
        let l = self.conditions.0.len();
        if l > 1{
            self.conditions.1.push((l,if logic{'a'}else{'o'}));
        }
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
    pub (crate) changes : HashMap<String,AlbaTypes>,
    pub (crate) conditions : (Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(usize,char)>)
}
impl EditRowBuilder {
    /// Set the container to the `EditRow` structure being built.
    pub fn put_container(&mut self, container: String){
        self.container = container;
    }
    /// Set a change to the change list of the `EditRow` structure being built.
    pub fn edit_column(&mut self, column: String,new_value : AlbaTypes){
        self.changes.insert(column, new_value);
    }
    /// Add a new condition to the condition chain of the `EditRow` structure being built.
    /// logic -> true = AND, false = OR 
    pub fn add_conditions(&mut self, condition: (String,LogicalOperator,AlbaTypes), logic: bool){
        self.conditions.0.push(condition);
        let l = self.conditions.0.len();
        if l > 1{
            self.conditions.1.push((l,if logic{'a'}else{'o'}));
        }
    }
    /// Finish the builder, returning the compiled `Search` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        let col_nam : Vec<String> = self.changes.keys().map(|f|f.clone()).collect();
        let col_val : Vec<AlbaTypes> = self.changes.values().map(|f|f.clone()).collect();
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
        let col_nam : Vec<String> = self.changes.keys().map(|f|f.clone()).collect();
        let col_val : Vec<AlbaTypes> = self.changes.values().map(|f|f.clone()).collect();
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
    /// Set the container to the `DeleteRow` structure being built.
    pub fn put_container(&mut self, container: String){
        self.container = container;
    }
    /// Add a new condition to the condition chain of the `DeleteRow` structure being built.
    /// logic -> true = AND, false = OR 
    pub fn add_conditions(&mut self, condition: (String,LogicalOperator,AlbaTypes), logic: bool){
        self.conditions.0.push(condition);
        let l = self.conditions.0.len();
        if l > 1{
            self.conditions.1.push((l,if logic{'a'}else{'o'}));
        }
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
    /// Set the container to the `DeleteContainer` structure being built.
    pub fn put_container(&mut self, container: String){
        self.container = container;
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
    pub(crate) value : HashMap<String,AlbaTypes>
}
impl CreateRowBuilder {
    /// Set the container to the `CreateRow` structure being built.
    pub fn put_container(&mut self, container: String){
        self.container = container;
    }

    pub fn insert_value(&mut self,column : String,value: AlbaTypes){
        self.value.insert(column, value);
    }
    /// Finish the builder, returning the compiled `CreateRow` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::CreateRow(CreateRow{
            container: self.container,
            col_nam: self.value.keys().map(|f|f.clone()).collect(),
            col_val: self.value.values().map(|f|f.clone()).collect()
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `CreateRow` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::CreateRow(CreateRow{
            container: self.container.clone(),
            col_nam: self.value.keys().map(|f|f.clone()).collect(),
            col_val: self.value.values().map(|f|f.clone()).collect()
        }).compile()? as CompiledAlba)
    }
}



pub struct CreateContainerBuilder{
    pub(crate) container : String,
    pub(crate) headers : HashMap<String,AlbaTypes>
}
impl CreateContainerBuilder {
    /// Set the container to the `CreateContainer` structure being built.
    pub fn put_container(&mut self, container: String){
        self.container = container;
    }

    /// Insert the metadata to creating a new container
    pub fn insert_header(&mut self,column_name : String,column_type: AlbaTypes){
        self.headers.insert(column_name, column_type);
    }
    /// Finish the builder, returning the compiled `CreateContainer` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::CreateContainer(CreateContainer{
            name: self.container,
            col_nam: self.headers.keys().map(|f|f.clone()).collect(),
            col_val: self.headers.values().map(|f|f.clone()).collect()
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `CreateContainer` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::CreateContainer(CreateContainer{
            name: self.container.clone(),
            col_nam: self.headers.keys().map(|f|f.clone()).collect(),
            col_val: self.headers.values().map(|f|f.clone()).collect()
        }).compile()? as CompiledAlba)
    }
}


pub struct CommitBuilder{
    pub(crate) container : Option<String>,
}
impl CommitBuilder {
    /// Set the container to the `Commit` structure being built.
    pub fn put_container(&mut self, container: String){
        self.container = Some(container);
    }

    /// Set the container you're commiting to
    pub fn set_container(&mut self,container : String){
        self.container = Some(container);
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
    /// Set the container to the `Rollback` structure being built.
    pub fn put_container(&mut self, container: String){
        self.container = Some(container);
    }

    /// Set the container you're rolling back
    pub fn set_container(&mut self,container : String){
        self.container = Some(container);
    }
    /// Finish the builder, returning the compiled `Rollback` bytes in the `CompiledAlba` type.
    pub fn finish(self) -> Result<CompiledAlba,Error>{
        Ok(Commands::Commit(Commit{
            container: self.container,
        }).compile()? as CompiledAlba)
    }
    /// Finish the builder, returning the compiled `Rollback` bytes in the `CompiledAlba` type.
    /// 
    /// The difference between this method and the `finish` is that by using this one you can compile multiple times to recicle the builder.
    pub fn cloned_finish(&self) -> Result<CompiledAlba,Error>{
        Ok(Commands::Commit(Commit{
            container: self.container.clone(),
        }).compile()? as CompiledAlba)
    }
}
