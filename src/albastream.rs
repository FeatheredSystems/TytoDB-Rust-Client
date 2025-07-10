use std::fmt;
use std::string::ToString;

pub type CompiledAlba = Vec<u8>;

#[derive(Debug)]
pub enum ErrorKind{
    Other,
    InvalidInput,
    UnexpectedEof
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
            ErrorKind::InvalidInput => "InvalidInput",
            ErrorKind::UnexpectedEof => "UnexpectedEof"
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