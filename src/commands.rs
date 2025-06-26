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
                let name = match String::from_utf8(input[2..2+name_len].to_vec()){
                    Ok(s) => s,
                    Err(_) => {
                        return Err(Error::new(ErrorKind::InvalidInput, "Failed to get string, invalid UTF-8"))
                    }
                };
                let col_name_len = input [name_len+3] as usize;
                let mut offset = col_name_len.clone();
                let mut col_nam = Vec::with_capacity(col_name_len);
                let mut col_val = Vec::with_capacity(col_name_len);
                for _ in 0..col_name_len{
                    let size = input[offset] as usize;
                    let string = match String::from_utf8(input[offset..(offset+size)].to_vec()){
                        Ok(s) => s,
                        Err(_) => {
                            return Err(Error::new(ErrorKind::InvalidInput, "Failed to get string, invalid UTF-8"))
                        }
                    };
                    col_nam.push(string);
                    offset += size;
                }
                let col_val_raw = input[offset..].to_vec();
                for i in col_val_raw{
                    col_val.push(AlbaTypes::from_id(i)?);
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
                    logic_vec.push((index, operator));
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
                Ok(Commands::DeleteContainer(DeleteContainer { container: String::from_utf8_lossy(&input[0..]).to_string() }))
            },
            5 => {
                Ok(Commands::Search(Search::decompile(&input[0..])?))
            },
            6 => {
                let have = input[1] != 0;
                if !have{
                    return Ok(Commands::Commit(Commit { container: None }))
                }
                let mut offset : usize = 2;
                let size = match input[offset]{
                    0 => {
                        offset += 1;
                        input[offset] as usize
                    },
                    1 => {
                        offset += 2;
                        let mut load : [u8;2] = [0u8;2];
                        load[..2].copy_from_slice(&input[(offset-2)..offset]);
                        u16::from_le_bytes(load) as usize
                    },
                    2 => {
                        offset += 4;
                        let mut load : [u8;4] = [0u8;4];
                        load[..4].copy_from_slice(&input[(offset-4)..offset]);
                        u32::from_le_bytes(load) as usize
                    },
                    3 => {
                        offset += 8;
                        let mut load : [u8;8] = [0u8;8];
                        load[..8].copy_from_slice(&input[(offset-8)..offset]);
                        u64::from_le_bytes(load) as usize
                    },
                    _ => {
                        return Err(Error::new(ErrorKind::InvalidInput, "Invalid dynamic integer metadata"))
                    }
                };
                let str = String::from_utf8_lossy(&input[offset..offset+size]).to_string();
                return Ok(Commands::Commit(Commit { container: Some(str) }))
            },
            7 => {
                let have = input[1] != 0;
                if !have{
                    return Ok(Commands::Rollback(Rollback { container: None }))
                }
                let mut offset : usize = 2;
                let size = match input[offset]{
                    0 => {
                        offset += 1;
                        input[offset] as usize
                    },
                    1 => {
                        offset += 2;
                        let mut load : [u8;2] = [0u8;2];
                        load[..2].copy_from_slice(&input[(offset-2)..offset]);
                        u16::from_le_bytes(load) as usize
                    },
                    2 => {
                        offset += 4;
                        let mut load : [u8;4] = [0u8;4];
                        load[..4].copy_from_slice(&input[(offset-4)..offset]);
                        u32::from_le_bytes(load) as usize
                    },
                    3 => {
                        offset += 8;
                        let mut load : [u8;8] = [0u8;8];
                        load[..8].copy_from_slice(&input[(offset-8)..offset]);
                        u64::from_le_bytes(load) as usize
                    },
                    _ => {
                        return Err(Error::new(ErrorKind::InvalidInput, "Invalid dynamic integer metadata"))
                    }
                };
                let str = String::from_utf8_lossy(&input[offset..offset+size]).to_string();
                return Ok(Commands::Rollback(Rollback { container: Some(str) }))
            }
            _ => {
                return Err(Error::new(ErrorKind::InvalidInput, "Invalid command metadata"))
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
            binary.extend_from_slice(&[i.id()]);
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
pub struct CreateContainer{
    pub name : String,
    pub col_nam : Vec<String>,
    pub col_val : Vec<AlbaTypes>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct CreateRow{
    pub col_nam : Vec<String>,
    pub col_val : Vec<AlbaTypes>,
    pub container : String
}
#[derive(Debug, Clone, PartialEq)]
pub struct EditRow{
    pub col_nam : Vec<String>,
    pub col_val : Vec<AlbaTypes>,
    pub container : String,
    pub conditions : (Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(usize,char)>)
}
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteRow{
    pub container : String,
    pub conditions : Option<(Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(usize,char)>)>
}
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteContainer{
    pub container : String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlbaContainer {
    Real(String),
    Virtual(Vec<Search>)
}

impl Compile for AlbaContainer{
    fn compile(&self) -> Result<Vec<u8>,Error> {
        match self{
            AlbaContainer::Real(r) => {
                if r.len() > MAX_CONTAINER_NAME_LENGTH{
                    return Err(Error::new(ErrorKind::InvalidInput, "AlbaContainer::Real exceeds the limit to container name"))
                }
                let mut bytes = vec![0u8];
                let din = DynamicInteger::from_usize(r.len());
                bytes.extend_from_slice(&din.compile());
                bytes.extend_from_slice(&r.as_bytes());
                Ok(bytes)
            },
            AlbaContainer::Virtual(searchs) => {
                let mut bytes = vec![1u8];
                let din = DynamicInteger::from_usize(searchs.len());
                bytes.extend_from_slice(&din.compile());
                bytes.extend_from_slice(&searchs.compile()?);
                Ok(bytes)
            },
        }
    }
}

impl StandAloneDecompile for AlbaContainer {
    type Output = AlbaContainer;
    fn decompile(input : &[u8]) -> Result<Self::Output,Error>{
        if input.len() < 3{
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid payload, a compiled alba container have at least 3 bytes of metadata."))
        }
        let mut offset = 0usize;
        let din = match input[1]{
            0 => {
                offset += 2;
                input[2] as usize
            }
            1 => {
                offset += 4;
                let mut load = [0u8;2];
                load[0..2].copy_from_slice(&input[2..4]);
                u16::from_le_bytes(load) as usize
            }
            2 => {
                offset += 6;
                let mut load = [0u8;4];
                load[0..4].copy_from_slice(&input[2..6]);
                u32::from_le_bytes(load) as usize
            }
            3 => {
                offset += 10;
                let mut load = [0u8;8];
                load[0..8].copy_from_slice(&input[2..10]);
                u64::from_le_bytes(load) as usize
            }, 
            _ => {
                return Err(Error::new(ErrorKind::InvalidInput, "Invalid DynamicInteger metadata"))
            }
        };
        Ok(
            match input[0]{
                0 => {
                    AlbaContainer::Real(String::from_utf8_lossy(&input[offset..(offset+din)]).to_string())
                },
                1 => {
                    let mut offset = offset;
                    let mut v: Vec<Search> = Vec::with_capacity(din);
                    for _ in 0..din {
                        if offset >= input.len() {
                            return Err(Error::new(ErrorKind::InvalidInput, "Unexpected EOF while reading Search element header"));
                        }

                        let (entry_size, used_bytes) = match input[offset] {
                            0 => (input[offset + 1] as usize, 2),
                            1 => {
                                let mut load = [0u8; 2];
                                load.copy_from_slice(&input[offset + 1..offset + 3]);
                                (u16::from_le_bytes(load) as usize, 3)
                            }
                            2 => {
                                let mut load = [0u8; 4];
                                load.copy_from_slice(&input[offset + 1..offset + 5]);
                                (u32::from_le_bytes(load) as usize, 5)
                            }
                            3 => {
                                let mut load = [0u8; 8];
                                load.copy_from_slice(&input[offset + 1..offset + 9]);
                                (u64::from_le_bytes(load) as usize, 9)
                            }
                            _ => return Err(Error::new(ErrorKind::InvalidInput, "Invalid DynamicInteger in Search")),
                        };

                        offset += 1 + used_bytes;

                        v.push(Search::decompile(&input[offset..offset+entry_size])?);

                        offset += entry_size;
                    }

                    AlbaContainer::Virtual(v)
                },
                _ => {
                    return Err(Error::new(ErrorKind::InvalidInput,"The first u8 must be in a 0-1 range, the entered is out of it."))
                }
            }
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
pub struct Search{
    pub container : Vec<AlbaContainer>,
    pub conditions : (Vec<(String,LogicalOperator,AlbaTypes)>,Vec<(usize,char)>),
    pub col_nam : Vec<String>,
}

impl Compile for Search {
    fn compile(&self) -> Result<Vec<u8>,Error> {

        if self.col_nam.len() > MAX_CONTAINER_COLUMN_COUNT{
            return Err(Error::new(ErrorKind::InvalidInput, "The column count exceed the limit"))
        }


        if self.conditions.0.len() > u8::MAX as usize{
            return Err(Error::new(ErrorKind::InvalidInput, "The condition count exceed the limit of 255"))
        }

        let mut binary = vec![5u8];
        binary.push(self.col_nam.len() as u8);
        for i in self.col_nam.iter(){
            binary.push(i.len() as u8);
            binary.extend_from_slice(i.as_bytes())
        }
        binary.push(self.conditions.0.len() as u8);
        for i in self.conditions.0.iter(){
            binary.push(i.0.len() as u8);
            binary.extend_from_slice(i.0.as_bytes());
            binary.push(i.1.id());
            binary.extend_from_slice(&i.2.as_bytes())
        }
        binary.extend_from_slice(&(self.conditions.1.len() as u8).to_le_bytes());
        for i in self.conditions.1.iter() {
            binary.push(i.0 as u8);
            binary.push(match i.1{
                'a'|'A' => 1,
                _ => 0
            })
        }
        binary.extend_from_slice(&self.container.compile()?);
        Ok(binary)
    }
}

impl StandAloneDecompile for Search {
    type Output = Search;
    fn decompile(input : &[u8]) -> Result<Self::Output,Error> {
        if input.len() < 1 || input[0] != 5u8{
            return Err(Error::new(ErrorKind::InvalidInput,"Invalid Search command metadata"));
        } 
        let cols = input[1] as usize;
        let mut col_names : Vec<String> = Vec::with_capacity(cols);
        let mut offset = 2usize;
        for _ in 0..cols{
            let string_size = input[offset] as usize;
            let txt = String::from_utf8_lossy(&input[offset+1..(offset+string_size+1)]).to_string();
            col_names.push(txt);
            offset += string_size + 1;
        }

        let mut conditions: (Vec<(String, LogicalOperator, AlbaTypes)>, Vec<(usize, char)>) = (Vec::new(),Vec::new());
        let condition_count = input[offset] as usize;
        offset += 1;
        for _ in 0..condition_count{
            let col_l = input[offset] as usize;
            offset += 1;
            let col_n = String::from_utf8_lossy(&input[(offset)..offset+col_l]).to_string();
            offset += col_l;
            let logo = LogicalOperator::from_id(input[offset])?;
            offset += 1;
            let abtp = input[offset].clone();
            offset += 1;
            let value = match abtp{
                0 => {
                    let dintid = input[offset]; 
                    let dint = match dintid{
                        0 => {
                            offset += 1;
                            input[offset] as usize
                        },
                        1 => {
                            let mut load = [0u8;2];
                            load[..2].copy_from_slice(&input[offset+1..offset+3]);
                            offset += 3;
                            let u = u16::from_le_bytes(load);
                            u as usize
                        },
                        2 => {
                            let mut load = [0u8;4];
                            load[..4].copy_from_slice(&input[offset+1..offset+5]);
                            let u = u32::from_le_bytes(load);
                            offset += 5;
                            u as usize
                        }
                        3 => {
                            let mut load = [0u8;8];
                            load[..8].copy_from_slice(&input[offset+1..offset+9]);
                            let u = u64::from_le_bytes(load);
                            offset += 9;
                            u as usize
                        },
                        _ => {
                            return Err(Error::new(ErrorKind::InvalidInput, "Invalid DynamicInteger metadata"))
                        }
                    };
                    let s = String::from_utf8_lossy(&input[offset..offset+dint]).to_string();
                    offset+= dint;
                    AlbaTypes::String(s)
                },
                1 => {
                    offset += 1;
                    AlbaTypes::U8(input[offset-1].clone())
                }
                8 => {
                    offset += 1;
                    AlbaTypes::Bool(input[offset-1] != 0)
                }
                2 => {
                    let mut load = [0u8;2];
                    load[..2].clone_from_slice(&input[offset..(offset+2)]);
                    offset += 2;
                    AlbaTypes::U16(u16::from_le_bytes(load))
                },
                3 => {
                    let mut load = [0u8;4];
                    load[..4].clone_from_slice(&input[offset..(offset+4)]);
                    offset += 4;
                    AlbaTypes::U32(u32::from_le_bytes(load))
                },
                6 => {
                    let mut load = [0u8;4];
                    load[..4].clone_from_slice(&input[offset..(offset+4)]);
                    offset += 4;
                    AlbaTypes::F32(f32::from_le_bytes(load))
                },
                7 => {
                    let mut load = [0u8;8];
                    load[..8].clone_from_slice(&input[offset..(offset+8)]);
                    offset += 8;
                    AlbaTypes::F64(f64::from_le_bytes(load))
                }
                4 => {
                    let mut load = [0u8;8];
                    load[..8].clone_from_slice(&input[offset..(offset+8)]);
                    offset += 8;
                    AlbaTypes::U64(u64::from_le_bytes(load))
                }
                5 => {
                    let mut load = [0u8;16];
                    load[..16].clone_from_slice(&input[offset..(offset+16)]);
                    offset += 16;
                    AlbaTypes::U128(u128::from_le_bytes(load))
                }
                _ => {
                    return Err(Error::new(ErrorKind::InvalidInput, "Invalid alba type metadata"))
                }
                

            };
            conditions.0.push((col_n,logo,value))
        }
        let len = input[offset];
        offset += 1;
        for _ in 0..len{
            conditions.1.push((input[offset] as usize,if input[offset + 1] == 0{'O'}else{'A'}));
            offset+=2;
        }

        let dynint_cnt_id = input[offset] as usize;
        let dynint = match dynint_cnt_id{
            0 => {
                offset += 1;
                dynint_cnt_id
            },
            1 => {
                let mut load = [0u8;2];
                load[..2].copy_from_slice(&input[offset..offset+2]);
                offset += 2;
                u16::from_le_bytes(load) as usize
            },
            2 => {
                let mut load = [0u8;4];
                load[..4].copy_from_slice(&input[offset..offset+4]);
                offset += 4;
                u32::from_le_bytes(load) as usize
            },
            3 => {
                let mut load = [0u8;8];
                load[..8].copy_from_slice(&input[offset..offset+8]);
                offset += 8;
                u64::from_le_bytes(load) as usize
            },
            _ => {
                return Err(
                    Error::new(ErrorKind::InvalidInput, "Invalid dynamic integer metadata")
                )
            }
        };
        let mut container_list : Vec<AlbaContainer> = Vec::new();

        for _ in 0..dynint{
            container_list.push(
                {
                    if input.len() < 3{
                        return Err(Error::new(ErrorKind::InvalidInput, "Invalid payload, a compiled alba container have at least 3 bytes of metadata."))
                    }
                    offset += 1;
                    let din = match input[offset]{
                        0 => {
                            offset += 2;
                            input[offset-1] as usize
                        }
                        1 => {
                            let mut load = [0u8;2];
                            load[0..2].copy_from_slice(&input[offset+2..offset+4]);
                            offset += 4;
                            u16::from_le_bytes(load) as usize
                        }
                        2 => {
                            let mut load = [0u8;4];
                            load[0..4].copy_from_slice(&input[2..6]);
                            u32::from_le_bytes(load) as usize
                        }
                        3 => {
                            let mut load = [0u8;8];
                            load[0..8].copy_from_slice(&input[offset+2..offset+10]);
                            offset += 10;
                            u64::from_le_bytes(load) as usize
                        }, 
                        _ => {
                            return Err(Error::new(ErrorKind::InvalidInput, "Invalid DynamicInteger metadata"))
                        }
                    };
                    match input[offset]{
                        0 => {
                            AlbaContainer::Real(String::from_utf8_lossy(&input[offset..(offset+din)]).to_string())
                        },
                        1 => {
                            let mut offset = offset;
                            let mut v: Vec<Search> = Vec::with_capacity(din);
                            for _ in 0..din {
                                if offset >= input.len() {
                                    return Err(Error::new(ErrorKind::InvalidInput, "Unexpected EOF while reading Search element header"));
                                }
        
                                let (entry_size, used_bytes) = match input[offset] {
                                    0 => (input[offset + 1] as usize, 2),
                                    1 => {
                                        let mut load = [0u8; 2];
                                        load.copy_from_slice(&input[offset + 1..offset + 3]);
                                        (u16::from_le_bytes(load) as usize, 3)
                                    }
                                    2 => {
                                        let mut load = [0u8; 4];
                                        load.copy_from_slice(&input[offset + 1..offset + 5]);
                                        (u32::from_le_bytes(load) as usize, 5)
                                    }
                                    3 => {
                                        let mut load = [0u8; 8];
                                        load.copy_from_slice(&input[offset + 1..offset + 9]);
                                        (u64::from_le_bytes(load) as usize, 9)
                                    }
                                    _ => return Err(Error::new(ErrorKind::InvalidInput, "Invalid DynamicInteger in Search")),
                                };
        
                                offset += 1 + used_bytes;
        
                                v.push(Search::decompile(&input[offset..offset+entry_size])?);
        
                                offset += entry_size;
                            }
        
                            AlbaContainer::Virtual(v)
                        },
                        _ => {
                            return Err(Error::new(ErrorKind::InvalidInput,"The first u8 must be in a 0-1 range, the entered is out of it."))
                        }
                    }
                    
                }
            );
        }
        
        Ok(Search { container: container_list, conditions, col_nam: col_names })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Commit{
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
            let len = DynamicInteger::from_usize(bytes_string.len()).compile();
            bytes.extend_from_slice(&len);
            bytes.extend_from_slice(&bytes_string);
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Rollback{
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
            let len = DynamicInteger::from_usize(bytes_string.len()).compile();
            bytes.extend_from_slice(&len);
            bytes.extend_from_slice(&bytes_string);
        }
        Ok(bytes)
    }
}
