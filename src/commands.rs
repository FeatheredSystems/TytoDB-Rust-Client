use crate::{albastream::{Error, ErrorKind}, dynamic_int::DynamicInteger, logical_operators::LogicalOperator, types::AlbaTypes};

#[derive(Debug, Clone, PartialEq)]
pub enum Commands{
    CreateContainer(CreateContainer),
    CreateRow(CreateRow),
    EditRow(EditRow),
    DeleteRow(DeleteRow),
    DeleteContainer(DeleteContainer),
    Search(Search),
    Commit(Commit),
    Rollback(Rollback),

    // subcommands
    BatchCreateRows(BatchCreateRows),
    Batch(Batch)
}
impl Commands{
    pub fn compile(&self) -> Result<Vec<u8>,Error>{
        match self{
            Commands::CreateContainer(struc) => struc.compile(),
            Commands::CreateRow(struc) => struc.compile(),
            Commands::EditRow(struc) => struc.compile(),
            Commands::DeleteRow(struc) => struc.compile(),
            Commands::DeleteContainer(struc) => struc.compile(),
            Commands::Search(struc) => struc.compile(),
            Commands::Commit(struc) => struc.compile(),
            Commands::Rollback(struc) => struc.compile(),
            Commands::BatchCreateRows(struc) => struc.compile(),
            Commands::Batch(struc) => struc.compile(),
        }
    }
    pub fn id(&self) -> u8{
        match self{
            Commands::CreateContainer(_) => 0,
            Commands::CreateRow(_) => 1,
            Commands::EditRow(_) => 2,
            Commands::DeleteRow(_) => 3,
            Commands::DeleteContainer(_) => 4,
            Commands::Search(_) => 5,
            Commands::Commit(_) => 6,
            Commands::Rollback(_) => 7,
            Commands::BatchCreateRows(_) => 8,
            Commands::Batch(_) => 9,
        }
    }
}

pub trait Compile {
    fn compile(&self) -> Result<Vec<u8>,Error>;    
}
pub trait StandAloneDecompile {
    type Output: Compile;
    fn decompile(input: &[u8]) -> Result<Self::Output, Error>;
}


const MAX_CONTAINER_NAME_LENGTH : usize = 100;
const MAX_CONTAINER_COLUMN_LENGTH : usize = 25;
const MAX_CONTAINER_COLUMN_COUNT : usize = u8::MAX as usize;

// DECOMPILE
impl Commands{
    pub fn decompile(input: &[u8]) -> Result<Commands, Error> {
        if input.len() < 2 {
            return Err(Error::new(ErrorKind::InvalidInput, "This is not a valid compiled binary"));
        }
        
        match input[0] {
            0 => {
                let name_len = input[1] as usize;
                if name_len + 2 > input.len(){
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        &format!(
                            "Unexpected end of input: expected at least {} bytes for name (got only {}).",
                            name_len + 2,
                            input.len()
                        ),
                    ));
                }
                let name = match String::from_utf8(input[2..2+name_len].to_vec()){
                    Ok(s) => s,
                    Err(_) => {
                        return Err(Error::new(ErrorKind::InvalidInput, "Failed to get string, invalid UTF-8"))
                    }
                };
                if !name.is_ascii(){
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "The name must be ASCII only"
                    ));
                }
                let col_name_len = input[name_len + 2] as usize;
                let mut offset = name_len + 3;
                let mut col_nam = Vec::with_capacity(col_name_len);
                let mut col_val = Vec::with_capacity(col_name_len);
                for _ in 0..col_name_len{
                    if offset >= input.len() {
                        return Err(Error::new(ErrorKind::UnexpectedEof, "Unexpected end of input while reading column name size"));
                    }
                    let size = input[offset] as usize;
                    if offset + 1 + size > input.len() {
                        return Err(Error::new(ErrorKind::UnexpectedEof, "Unexpected end of input while reading column name data"));
                    }
                    let string = match String::from_utf8(input[(offset+1)..(offset+1+size)].to_vec()){
                        Ok(s) => s,
                        Err(_) => {
                            return Err(Error::new(ErrorKind::InvalidInput, "Failed to get string, invalid UTF-8"))
                        }
                    };
                    col_nam.push(string);
                    offset += size + 1; // +1 for the size byte
                }
                let col_val_raw = input[offset..].to_vec();
                for i in col_val_raw{
                    col_val.push(i);
                }
                Ok(Commands::CreateContainer(CreateContainer{
                    name,
                    col_nam,
                    col_val
                }))
            }
            1 => {
                let name_length = input[1] as usize;
                let name = &input[2..2+name_length];
                let mut offset = 2 + name_length; // directly after name
                let col_count = input[offset] as usize;
                offset += 1;
                let mut column_name : Vec<String> = Vec::with_capacity(col_count);
                for _ in 0..col_count{
                    let column_name_length = input[offset] as usize;
                    let cn = &input[offset+1..offset+1+column_name_length];
                    offset += column_name_length + 1;
                    column_name.push(String::from_utf8_lossy(cn).to_string());
                }
                let mut col_val = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    let (value, consumed) = AlbaTypes::from_bytes(&input[offset..])?;
                    col_val.push(value);
                    offset += consumed ;
                }
                
                Ok(Commands::CreateRow(CreateRow{
                    col_nam: column_name,
                    col_val,
                    container: String::from_utf8_lossy(name).to_string()
                }))
            }
            2 => {
                let name_length = input[1] as usize;
                let name = &input[2..2+name_length];
                let mut offset = 3+name_length;
                let col_count: usize = input[2 + name_length] as usize;
                let mut column_name : Vec<String> = Vec::with_capacity(col_count);
                for _ in 0..col_count{
                    let column_name_length = input[offset] as usize;
                    let cn = &input[offset+1..offset+1+column_name_length];
                    offset += column_name_length + 1;
                    column_name.push(String::from_utf8_lossy(cn).to_string());
                }
                let mut col_val = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    let (value, consumed) = AlbaTypes::from_bytes(&input[offset..])?;
                    col_val.push(value);
                    offset += consumed;
                }
                
                let conditions_count = input[offset] as usize;
                offset += 1;
                let mut conditions_vec = Vec::with_capacity(conditions_count);
                for _ in 0..conditions_count {
                    let condition_name_length = input[offset] as usize;
                    offset += 1;
                    let condition_name = String::from_utf8_lossy(&input[offset..offset+condition_name_length]).to_string();
                    offset += condition_name_length;
                    let logical_op = LogicalOperator::from_id(input[offset])?;
                    offset += 1;
                    let (condition_value, consumed) = AlbaTypes::from_bytes(&input[offset..])?;
                    offset += consumed;
                    conditions_vec.push((condition_name, logical_op, condition_value));
                }
                
                let logic_count = input[offset] as usize;
                offset += 1;
                let mut logic_vec = Vec::with_capacity(logic_count);
                for _ in 0..logic_count {
                    let index = input[offset] as usize;
                    offset += 1;
                    let operator = match input[offset] {
                        1 => 'A',
                        0 => 'O',
                        _ => 'O'
                    };
                    offset += 1;
                    logic_vec.push((index as u8, operator));
                }
                
                Ok(Commands::EditRow(EditRow{
                    col_nam: column_name,
                    col_val,
                    container: String::from_utf8_lossy(name).to_string(),
                    conditions: (conditions_vec, logic_vec)
                }))
            }
            3 => {
                let name_length = input[1] as usize;
                let name = &input[2..2+name_length];
                let mut offset = 2 + name_length;
                
                let has_conditions = input[offset] != 0;
                offset += 1;
                
                let conditions = if has_conditions {
                    let conditions_count = input[offset] as usize;
                    offset += 1;
                    let mut conditions_vec = Vec::with_capacity(conditions_count);
                    
                    for _ in 0..conditions_count {
                        let condition_name_length = input[offset] as usize;
                        offset += 1;
                        let condition_name = String::from_utf8_lossy(&input[offset..offset+condition_name_length]).to_string();
                        offset += condition_name_length;
                        let logical_op = LogicalOperator::from_id(input[offset])?;
                        offset += 1;
                        let (condition_value, consumed) = AlbaTypes::from_bytes(&input[offset..])?;
                        offset += consumed;
                        conditions_vec.push((condition_name, logical_op, condition_value));
                    }
                    
                    let logic_count = input[offset] as usize;
                    offset += 1;
                    let mut logic_vec = Vec::with_capacity(logic_count);
                    for _ in 0..logic_count {
                        let index = input[offset] as usize;
                        offset += 1;
                        let operator = match input[offset] {
                            1 => 'A',
                            0 => 'O',
                            _ => 'O'
                        };
                        offset += 1;
                        logic_vec.push((index, operator));
                    }
                    
                    Some((conditions_vec, logic_vec))
                } else {
                    None
                };
                
                Ok(Commands::DeleteRow(DeleteRow {
                    container: String::from_utf8_lossy(name).to_string(),
                    conditions
                }))
            }
            
            4 => {
                Ok(Commands::DeleteContainer(DeleteContainer { container: String::from_utf8_lossy(&input[1..]).to_string() }))
            },
            5 => {
                Ok(Commands::Search(Search::decompile(&input[0..])?))
            },
            6 => {
                let have = input[1] != 0;
                if !have{
                    return Ok(Commands::Commit(Commit { container: None }))
                }
                let size = input[2] as usize;
                let str = String::from_utf8_lossy(&input[3..3+size]).to_string();
                Ok(Commands::Commit(Commit { container: Some(str) }))
            },
            7 => {
                let have = input[1] != 0;
                if !have{
                    return Ok(Commands::Rollback(Rollback { container: None }))
                }
                let size = input[2] as usize;
                let str = String::from_utf8_lossy(&input[3..3+size]).to_string();
                Ok(Commands::Rollback(Rollback { container: Some(str) }))
            },
            8 => {
                let name_length = input[1] as usize;
                let name = &input[2..2+name_length];
                let mut offset = 2 + name_length; // directly after name
                let col_count = input[offset] as usize;
                offset += 1;
                let mut column_name : Vec<String> = Vec::with_capacity(col_count);
                for _ in 0..col_count{
                    let column_name_length = input[offset] as usize;
                    let cn = &input[offset+1..offset+1+column_name_length];
                    offset += column_name_length + 1;
                    column_name.push(String::from_utf8_lossy(cn).to_string());
                }
                let mut group_col_val = Vec::with_capacity(col_count);
                let batched_count = {
                    let mut load = [0u8;4];
                    load[..].copy_from_slice(&input[offset..offset+4]);
                    offset += 4;
                    u32::from_le_bytes(load)
                } ;
                for _ in 0..batched_count{ 
                    let mut col_val = Vec::new();
                    for _ in 0..col_count {
                        let (value, consumed) = AlbaTypes::from_bytes(&input[offset..])?;
                        col_val.push(value);
                        offset += consumed ;
                    }
                    col_val.shrink_to_fit();
                    group_col_val.push(col_val);
                }
                group_col_val.shrink_to_fit();
                
                Ok(Commands::BatchCreateRows(BatchCreateRows{
                    col_nam: column_name,
                    col_val: group_col_val,
                    container: String::from_utf8_lossy(name).to_string()
                }))
            },
            9 => {
                Ok(Commands::Batch(Batch::decompile(input)?))
            }
            _ => {
                Err(Error::new(ErrorKind::InvalidInput, "Invalid command metadata"))
            }
        }
    }
}
impl Compile for CreateContainer {
    fn compile(&self) -> Result<Vec<u8>,Error> {
        if self.name.len() > MAX_CONTAINER_NAME_LENGTH{
            return Err(Error::new(ErrorKind::InvalidInput,"Invalid container name, the maximum length of a container name is 100 and the entered exceeded the value."))
        }
        if self.col_nam.len() != self.col_val.len(){
            return Err(Error::new(ErrorKind::InvalidInput, "Mismatch on the column name count and column value count, both lengths have to be the same."))
        }
        if self.col_nam.len() > MAX_CONTAINER_COLUMN_COUNT{
            return Err(Error::new(ErrorKind::InvalidInput, "The column count exceed the limit"))
        }
        for i in self.col_nam.iter(){
            if i.len() > MAX_CONTAINER_COLUMN_LENGTH{
                return Err(Error::new(ErrorKind::InvalidInput, "A column name length exceed the limit"))
            }
        }
        let mut binary : Vec<u8> = Vec::new();
        binary.extend_from_slice(&0u8.to_le_bytes());

        binary.extend_from_slice(&(self.name.len() as u8).to_le_bytes());
        binary.extend_from_slice(&self.name.as_bytes());

        binary.extend_from_slice(&(self.col_nam.len() as u8).to_le_bytes());
        for i in &self.col_nam{
            binary.extend_from_slice(&(i.len() as u8).to_le_bytes());
            binary.extend_from_slice(&i.as_bytes());
        }

        for i in &self.col_val{
            binary.push(*i);
        }


        Ok(binary)
    }
}
impl Compile for CreateRow{
    fn compile(&self) -> Result<Vec<u8>,Error> {
        if self.container.len() > MAX_CONTAINER_NAME_LENGTH{
            return Err(Error::new(ErrorKind::InvalidInput,"Invalid container name, the maximum length of a container name is 100 and the entered exceeded the value."))
        }
        if self.col_nam.len() != self.col_val.len(){
            return Err(Error::new(ErrorKind::InvalidInput, "Mismatch on the column name count and column value count, both lengths have to be the same."))
        }
        if self.col_nam.len() > MAX_CONTAINER_COLUMN_COUNT{
            return Err(Error::new(ErrorKind::InvalidInput, "The column count exceed the limit"))
        }
        let mut binary = vec![1u8];
        binary.push(self.container.len() as u8);
        binary.extend_from_slice(self.container.as_bytes());
        binary.push(self.col_nam.len() as u8);
        for i in self.col_nam.iter(){
            binary.push(i.len() as u8);
            binary.extend_from_slice(i.as_bytes())
        }
        for i in self.col_val.iter(){
            binary.extend_from_slice(&i.as_bytes())
        }
        Ok(binary)
    }
}
impl Compile for EditRow{
    fn compile(&self) -> Result<Vec<u8>,Error> {
        if self.container.len() > MAX_CONTAINER_NAME_LENGTH{
            return Err(Error::new(ErrorKind::InvalidInput,"Invalid container name, the maximum length of a container name is 100 and the entered exceeded the value."))
        }
        if self.col_nam.len() != self.col_val.len(){
            return Err(Error::new(ErrorKind::InvalidInput, "Mismatch on the column name count and column value count, both lengths have to be the same."))
        }
        if self.col_nam.len() > MAX_CONTAINER_COLUMN_COUNT{
            return Err(Error::new(ErrorKind::InvalidInput, "The column count exceed the limit"))
        }
        if self.conditions.0.len() > u8::MAX as usize{
            return Err(Error::new(ErrorKind::InvalidInput, "The condition count exceed the limit of 255"))
        }
        let mut binary = vec![2u8];
        binary.push(self.container.len() as u8);
        binary.extend_from_slice(self.container.as_bytes());
        binary.push(self.col_nam.len() as u8);
        for i in self.col_nam.iter(){
            binary.push(i.len() as u8);
            binary.extend_from_slice(i.as_bytes())
        }
        for i in self.col_val.iter(){
            binary.extend_from_slice(&i.as_bytes())
        }

        binary.push(self.conditions.0.len() as u8);
        for i in self.conditions.0.iter(){
            binary.push(i.0.len() as u8);
            binary.extend_from_slice(i.0.as_bytes());
            binary.push(i.1.id());
            binary.extend_from_slice(&i.2.as_bytes())
        }
        binary.push(self.conditions.1.len() as u8);
        for i in self.conditions.1.iter(){
            binary.push(i.0 as u8);
            binary.push(match i.1{
                'A'|'a' => 1,
                'O'|'o' => 0,
                _ => 0
            });
        }
        Ok(binary)
    }
}
impl Compile for DeleteRow{
    fn compile(&self) -> Result<Vec<u8>,Error> {
        if self.container.len() > MAX_CONTAINER_NAME_LENGTH{
            return Err(Error::new(ErrorKind::InvalidInput,"Invalid container name, the maximum length of a container name is 100 and the entered exceeded the value."))
        }
        if let Some(c) = &self.conditions{
            if c.0.len() > u8::MAX as usize{
                return Err(Error::new(ErrorKind::InvalidInput, "The condition count exceed the limit of 255"))
            }
        } 
        let mut binary = vec![3u8];
        binary.push(self.container.len() as u8);
        binary.extend_from_slice(self.container.as_bytes());
        binary.push(self.conditions.is_some() as u8);
        if let Some(conditions) = &self.conditions{
            binary.push(conditions.0.len() as u8);
            for i in conditions.0.iter(){
                binary.push(i.0.len() as u8);
                binary.extend_from_slice(i.0.as_bytes());
                binary.push(i.1.id());
                binary.extend_from_slice(&i.2.as_bytes())
            }
            binary.push(conditions.1.len() as u8);
            for i in conditions.1.iter(){
                binary.push(i.0 as u8);
                binary.push(match i.1{
                    'A'|'a' => 1,
                    'O'|'o' => 0,
                    _ => 0
                });
            }
        }
        Ok(binary)
    }
}
impl Compile for DeleteContainer{
    fn compile(&self) -> Result<Vec<u8>,Error> {
        if self.container.len() > MAX_CONTAINER_NAME_LENGTH{
            return Err(Error::new(ErrorKind::InvalidInput, "The entered container name exceed the limit"))
        }
        let mut binary = vec![4u8];
        binary.extend_from_slice(self.container.as_bytes());
        Ok(binary)
    }
}


#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CreateContainer{
    pub name : String,
    pub col_nam : Vec<String>,
    pub col_val : Vec<u8>,
}
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CreateRow{
    pub col_nam : Vec<String>,
    pub col_val : Vec<AlbaTypes>,
    pub container : String
}
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EditRow{
    pub col_nam : Vec<String>,
    pub col_val : Vec<AlbaTypes>,
    pub container : String,
    pub conditions : (Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(u8,char)>)
}
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DeleteRow{
    pub container : String,
    pub conditions : Option<(Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(usize,char)>)>
}
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DeleteContainer{
    pub container : String,
}


pub type AlbaContainer = String;

impl Compile for AlbaContainer{
    fn compile(&self) -> Result<Vec<u8>,Error> {
        if self.len() > MAX_CONTAINER_NAME_LENGTH{
            return Err(Error::new(ErrorKind::InvalidInput, "AlbaContainer::Real exceeds the limit to container name"))
        }
        let mut a = vec![];
        a.push(self.len()as u8);
        a.extend_from_slice(&self.as_bytes());
        Ok(a)
            
    }
}

impl StandAloneDecompile for AlbaContainer {
    type Output = AlbaContainer;
    fn decompile(input : &[u8]) -> Result<Self::Output,Error>{
        if input.len() < 3{
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid payload, a compiled alba container have at least 3 bytes of metadata."))
        }
        let offset = 0usize;
        Ok(
            (String::from_utf8_lossy(&input[offset+1 as usize .. offset + 1 + input[offset] as usize]).to_string()) as AlbaContainer
        )
    }
}

impl <T:Compile+StandAloneDecompile> Compile for Vec<T>  {
    fn compile(&self) -> Result<Vec<u8>,Error> {
        let mut bytes : Vec<u8> = Vec::new();
        let len = DynamicInteger::from_usize(self.len());
        bytes.extend_from_slice(&len.compile());
        for i in self{
            let b = i.compile()?;
            let size = DynamicInteger::from_usize(b.len());
            bytes.extend_from_slice(&size.compile());
            bytes.extend_from_slice(&b);
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Search{
    pub container : AlbaContainer,
    pub conditions : (Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(u8,char)>),
    pub col_nam : Vec<String>,
}

impl Compile for Search {
    fn compile(&self) -> Result<Vec<u8>,Error> {
        let mut bytes = vec![5u8];
        if self.col_nam.len() > 255{
            return  Err(Error::new(ErrorKind::InvalidInput, "Column name count cannot be higher than 255"));
        }
        bytes.push(self.col_nam.len() as u8);
        for i in self.col_nam.iter(){
            if i.len() > 255{
                return Err(Error::new(ErrorKind::InvalidInput, "Column names lengths cannot be higher than 255"))
            }
            bytes.push(i.len() as u8);
            bytes.extend_from_slice(&i.as_bytes());
        }
        if self.conditions.0.len() > 255{
            return Err(Error::new(ErrorKind::InvalidInput, "Conditions count cannot be higher than 255"))
        }
        bytes.push(self.conditions.0.len() as u8);
        for i in self.conditions.0.iter(){
            bytes.push(i.0.len() as u8);
            bytes.extend_from_slice(&i.0.as_bytes());
            bytes.push(i.1.id());
            bytes.extend_from_slice(&i.2.as_bytes());
        }
        for i in self.conditions.1.iter(){
            bytes.push(i.0);
            bytes.push(if i.1 == 'a' || i.1 == 'A'{1u8}else{0u8})
        }
        let b = self.container.compile()?;
        bytes.extend_from_slice(&(b.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&b);
        Ok(bytes)
    }
}

impl StandAloneDecompile for Search {
    type Output = Search;
    fn decompile(input: &[u8]) -> Result<Self::Output, Error> {
        if input.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "Input is empty"));
        }
        
        if input[0] != 5u8 {
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid magic byte"));
        }
        
        let mut offset = 1usize;
        
        if offset >= input.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Unexpected end of input"));
        }
        
        let column_count = input[offset] as usize;
        offset += 1;
        let mut columns: Vec<String> = Vec::new();
        
        for _ in 0..column_count {
            if offset >= input.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Unexpected end of input"));
            }
            let size = input[offset] as usize;
            offset += 1;
            
            if offset + size > input.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Column name extends beyond input"));
            }
            
            let str = String::from_utf8_lossy(&input[offset..offset + size]).to_string();
            offset += size;
            columns.push(str);
        }
        
        if offset >= input.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Unexpected end of input"));
        }
        
        let conditions_count = input[offset] as usize;
        offset += 1;
        
        let mut conditions = (Vec::new(), Vec::new());
        
        for _ in 0..conditions_count {
            if offset >= input.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Unexpected end of input"));
            }
            
            let subject_column_size = input[offset] as usize;
            offset += 1;
            
            if offset + subject_column_size > input.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Subject column name extends beyond input"));
            }
            
            let subject_column_name = String::from_utf8_lossy(&input[offset..offset + subject_column_size]).to_string();
            offset += subject_column_size;
            
            if offset >= input.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Unexpected end of input"));
            }
            
            let logical_operator = LogicalOperator::from_id(input[offset])?;
            offset += 1;
            
            let (value, consumed) = AlbaTypes::from_bytes(&input[offset..])?;
            offset += consumed;
            
            conditions.0.push((subject_column_name, logical_operator, value));
        }
        
        if offset < input.len() {
            let available_logic_gate_bytes = input.len() - offset;
            let expected_logic_gate_bytes = conditions_count * 2;
            
            if available_logic_gate_bytes >= expected_logic_gate_bytes {
                for _ in 0..conditions_count {
                    let index_pointer = input[offset];
                    let logic_gate = if input[offset + 1] == 0 { 'o' } else { 'a' };
                    conditions.1.push((index_pointer, logic_gate));
                    offset += 2;
                }
            } else {
                eprintln!("Warning: Expected {} logic gate bytes, but only {} available", 
                         expected_logic_gate_bytes, available_logic_gate_bytes);
            }
        }
        
        if offset >= input.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Unexpected end of input"));
        }
        
        if offset + 8 > input.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Not enough bytes for container length"));
        }
        
        let mut length_bytes = [0u8; 8];
        length_bytes.copy_from_slice(&input[offset..offset + 8]);
        let container_length = u64::from_le_bytes(length_bytes) as usize;
        offset += 8;
        
        if offset + container_length > input.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Container data extends beyond input"));
        }
        
        let container_data = &input[offset..offset + container_length];
        
        let container = AlbaContainer::decompile(container_data)?;
        
        Ok(Search { 
            container, 
            conditions, 
            col_nam: columns 
        })
    }
}



#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Commit{
    pub container : Option<String>,
}

impl Compile for Commit{
    fn compile(&self) -> Result<Vec<u8>,Error> {
        let mut bytes = vec![6u8,self.container.is_some() as u8];
        if let Some(co) = &self.container{
            if co.len() > MAX_CONTAINER_NAME_LENGTH{
                return Err(Error::new(ErrorKind::InvalidInput, "The container name exceed the limit"))
            }
            let bytes_string = co.as_bytes();
            let len = bytes_string.len() as u8;
            bytes.push(len);
            bytes.extend_from_slice(&bytes_string);
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Rollback{
    pub container : Option<String>,
}
impl Compile for Rollback{
    fn compile(&self) -> Result<Vec<u8>,Error> {
        let mut bytes = vec![7u8,self.container.is_some() as u8];
        if let Some(co) = &self.container{
            if co.len() > MAX_CONTAINER_NAME_LENGTH{
                return Err(Error::new(ErrorKind::InvalidInput, "The container name exceed the limit"))
            }
            let bytes_string = co.as_bytes();
            bytes.push(bytes_string.len() as u8);
            bytes.extend_from_slice(&bytes_string);
        }
        Ok(bytes)
    }
}


#[derive(Debug,Clone,PartialEq)]
pub(crate) struct BatchCreateRows{
    pub col_nam : Vec<String>,
    pub col_val : Vec<Vec<AlbaTypes>>,
    pub container : String
}

impl Compile for BatchCreateRows{
    fn compile(&self) -> Result<Vec<u8>,Error> {
        if self.container.len() > MAX_CONTAINER_NAME_LENGTH{
            return Err(Error::new(ErrorKind::InvalidInput,"Invalid container name, the maximum length of a container name is 100 and the entered exceeded the value."))
        }
        if self.col_nam.len() > MAX_CONTAINER_COLUMN_COUNT{
            return Err(Error::new(ErrorKind::InvalidInput, "The column count exceed the limit"))
        }
        let mut binary = vec![8u8];
        binary.push(self.container.len() as u8);
        binary.extend_from_slice(self.container.as_bytes());
        binary.push(self.col_nam.len() as u8);
        for i in self.col_nam.iter(){
            binary.push(i.len() as u8);
            binary.extend_from_slice(i.as_bytes())
        }

        let l = self.col_val.len() as u32;
        binary.extend_from_slice(&l.to_le_bytes());
        
        for ldoajfg in self.col_val.iter(){
            for i in ldoajfg.iter(){
                binary.extend_from_slice(&i.as_bytes())
            }
        }
        Ok(binary)
    }
}

#[derive(Debug,Clone,PartialEq)]
pub(crate) struct Batch{
    pub transaction : bool,
    pub commands : Vec<Commands>,
}
impl Compile for Batch{
    fn compile(&self) -> Result<Vec<u8>,Error>{
        let mut binary_ex = vec![9u8]; // operaton id
        if self.commands.len() > 2147483648{
            return Err(Error::new(ErrorKind::InvalidInput, "The command count cannot exceed 2147483648"))
        }
        let mut count = 0i32;
        let mut bins = Vec::new();
        for i in self.commands.iter(){
            bins.push(i.compile()?);
            count += 1;
        }
        if self.transaction{count*=-1}
        binary_ex.extend_from_slice(&count.to_le_bytes());
        for i in bins{
            let len = i.len() as u32;
            binary_ex.extend_from_slice(&len.to_le_bytes());
            binary_ex.extend_from_slice(&i);
        }
        Ok(binary_ex)
    }
}
impl Batch{
    pub(crate) fn decompile(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "Empty byte array"));
        }
        
        if bytes[0] != 9u8 {
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid operation id"));
        }
        
        if bytes.len() < 5 {
            return Err(Error::new(ErrorKind::InvalidInput, "Insufficient data for command count"));
        }
        
        let count_bytes = [bytes[1], bytes[2], bytes[3], bytes[4]];
        let count = i32::from_le_bytes(count_bytes);
        
        let transaction = count < 0;
        let command_count = count.abs() as usize;
        
        let mut commands: Vec<Commands> = Vec::with_capacity(command_count);
        let mut offset = 5;        
        for _ in 0..command_count {
            if offset + 4 > bytes.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Insufficient data for command length"));
            }
            
            let len_bytes = [
                bytes[offset], 
                bytes[offset + 1], 
                bytes[offset + 2], 
                bytes[offset + 3]
            ];
            let command_len = u32::from_le_bytes(len_bytes) as usize;
            offset += 4;
            
            if offset + command_len > bytes.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Insufficient data for command"));
            }
            
            let command_bytes = &bytes[offset..offset + command_len];
            let command = Commands::decompile(command_bytes)?;
            commands.push(command);
            
            offset += command_len;
        }
        
        Ok(Batch {
            transaction,
            commands,
        })
    }

}
