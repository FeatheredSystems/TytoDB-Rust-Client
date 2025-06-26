use std::fmt;
use std::string::ToString;

use crate::{commands::Commands, dynamic_int::DynamicInteger};

pub type CompiledAlba = Vec<u8>;

/// The compiling process compress the final bytes by default with zstd algorithm, the level One is 1, Low is 3, Medium 6, high is 15 and Extreme 22 (maximum zstd provide).
pub enum AlbaStreamCompressionLevel {
    One,
    Low,
    Medium,
    High,
    Extreme 
}

pub struct AlbaStream{
    pub commands : Vec<Commands>,
    pub compression_lvl : AlbaStreamCompressionLevel
}
impl AlbaStream{
    pub fn compile(self) -> Result<CompiledAlba,Error>{
        let mut compiled_binary : Vec<u8> = Vec::new();
        let command_count = DynamicInteger::from_usize(self.commands.len()).compile();
        compiled_binary.extend_from_slice(&command_count);
        for i in self.commands{
            let binary = i.compile()?;
            let bin_size = DynamicInteger::from_usize(binary.len()).compile();
            compiled_binary.extend_from_slice(&bin_size);
            compiled_binary.extend_from_slice(&binary);
        }
        let compiled_binary: Vec<u8> = if let Ok(b) = zstd::bulk::compress(&compiled_binary, match self.compression_lvl{
            AlbaStreamCompressionLevel::One => 1, 
            AlbaStreamCompressionLevel::Low => 3,
            AlbaStreamCompressionLevel::Medium => 6, 
            AlbaStreamCompressionLevel::High => 15, 
            AlbaStreamCompressionLevel::Extreme => 22
        }){
            b
        }else{
            return Err(Error::new(ErrorKind::Other, "Failed to compress the compiled binary"))
        };
        Ok(compiled_binary as CompiledAlba)
    }
}

#[derive(Debug)]
pub enum ErrorKind{
    Other,
    InvalidInput
}

pub struct Error{
    kind : ErrorKind,
    message : String
}
impl Error{
    pub fn new(kind : ErrorKind, message : &str) -> Error{
        Error{kind,message:message.to_string()}
    }
}
impl ToString for Error {
    fn to_string(&self) -> String {
        return format!("Error<Kind:{}>: {}",
        match self.kind{
            ErrorKind::Other => "Other",
            ErrorKind::InvalidInput => "InvalidInput"
        },
        self.message.to_string()
    )
    }
}


impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}