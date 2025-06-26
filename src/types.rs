use crate::{albastream::{Error, ErrorKind}, dynamic_int::{vec_from_two_vec, DynamicInteger}};


#[derive(Debug, Clone, PartialEq)]
pub enum AlbaTypes{
    String(String),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    F32(f32),
    F64(f64),
    Bool(bool),
}

impl AlbaTypes{
    pub fn from_id(id : u8) -> Result<AlbaTypes,Error>{
        Ok(match id{
            0 => AlbaTypes::String(String::new()),
            1 => AlbaTypes::U8(0),
            2 => AlbaTypes::U16(0),
            3 => AlbaTypes::U32(0),
            4 => AlbaTypes::U64(0),
            5 => AlbaTypes::U128(0),
            6 => AlbaTypes::F32(0.0),
            7 => AlbaTypes::F64(0.0),
            8 => AlbaTypes::Bool(false),
            _ => return Err(Error::new(ErrorKind::InvalidInput, "Invalid AlbaType id"))
        })
    }
    pub fn id(&self) -> u8{
        match self{
            AlbaTypes::String(_) => 0u8,
            AlbaTypes::U8(_) => 1u8,
            AlbaTypes::U16(_) => 2u8,
            AlbaTypes::U32(_) => 3u8,
            AlbaTypes::U64(_) => 4u8,
            AlbaTypes::U128(_) => 5u8,
            AlbaTypes::F32(_) => 6u8,
            AlbaTypes::F64(_) => 7u8,
            AlbaTypes::Bool(_) => 8u8,
        }
    }
    pub fn as_bytes(&self) -> Vec<u8>{
        match self{
            AlbaTypes::String(s) => {
                let mut b = 0u8.to_le_bytes().to_vec();
                b.extend_from_slice(&vec_from_two_vec(&DynamicInteger::from_usize(s.len()).compile(),s.as_bytes()));
                b
            },
            AlbaTypes::U8(v) => {
                let mut b = 1u8.to_le_bytes().to_vec();
                b.extend_from_slice(&v.to_le_bytes());
                b
            }
            AlbaTypes::U16(v) => {
                let mut b = 2u8.to_le_bytes().to_vec();
                b.extend_from_slice(&v.to_le_bytes());
                b
            }
            AlbaTypes::U32(v) => {
                let mut b = 3u8.to_le_bytes().to_vec();
                b.extend_from_slice(&v.to_le_bytes());
                b
            }
            AlbaTypes::U64(v) => {
                let mut b = 4u8.to_le_bytes().to_vec();
                b.extend_from_slice(&v.to_le_bytes());
                b
            }
            AlbaTypes::U128(v) => {
                let mut b = 5u8.to_le_bytes().to_vec();
                b.extend_from_slice(&v.to_le_bytes());
                b
            }
            AlbaTypes::F32(v) => {
                let mut b = 6u8.to_le_bytes().to_vec();
                b.extend_from_slice(&v.to_le_bytes());
                b
            }
            AlbaTypes::F64(v) => {
                let mut b = 7u8.to_le_bytes().to_vec();
                b.extend_from_slice(&v.to_le_bytes());
                b
            }
            AlbaTypes::Bool(v) => {
                let mut b = 8u8.to_le_bytes().to_vec();
                b.extend_from_slice(&(*v as u8).to_le_bytes());
                b
            }
        }
    }
}
impl AlbaTypes {
    pub fn from_bytes(input: &[u8]) -> Result<(Self, usize), Error> {
        if input.len() < 2 {
            return Err(Error::new(ErrorKind::InvalidInput, "Input too short to decode AlbaType"));
        }

        let id = input[0];
        match id {
            0 => {
                let length_type = input[1];
                let (dynint, header_size): (DynamicInteger, usize) = match length_type {
                    0 => {
                        if input.len() < 3 {
                            return Err(Error::new(ErrorKind::InvalidInput, "Truncated U8 dynamic int"));
                        }
                        (DynamicInteger::U8((0, input[2])), 3)
                    }
                    1 => {
                        if input.len() < 5 {
                            return Err(Error::new(ErrorKind::InvalidInput, "Truncated U16 dynamic int"));
                        }
                        let mut load = [0u8; 2];
                        load.copy_from_slice(&input[2..4]);
                        (DynamicInteger::U16((1, u16::from_le_bytes(load))), 4)
                    }
                    2 => {
                        if input.len() < 7 {
                            return Err(Error::new(ErrorKind::InvalidInput, "Truncated U32 dynamic int"));
                        }
                        let mut load = [0u8; 4];
                        load.copy_from_slice(&input[2..6]);
                        (DynamicInteger::U32((2, u32::from_le_bytes(load))), 6)
                    }
                    3 => {
                        if input.len() < 11 {
                            return Err(Error::new(ErrorKind::InvalidInput, "Truncated U64 dynamic int"));
                        }
                        let mut load = [0u8; 8];
                        load.copy_from_slice(&input[2..10]);
                        (DynamicInteger::U64((3, u64::from_le_bytes(load))), 10)
                    }
                    _ => return Err(Error::new(ErrorKind::InvalidInput, "Invalid dynamic string length type")),
                };
                let str_len = dynint.usize();
                let total_size = header_size + str_len;

                if input.len() < total_size {
                    return Err(Error::new(ErrorKind::InvalidInput, "String data truncated"));
                }

                let string = String::from_utf8_lossy(&input[header_size..total_size]).to_string();
                Ok((AlbaTypes::String(string), total_size))
            }

            1 => {
                if input.len() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Truncated U8"));
                }
                Ok((AlbaTypes::U8(input[1]), 2))
            }

            2 => {
                if input.len() < 3 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Truncated U16"));
                }
                let mut buf = [0u8; 2];
                buf.copy_from_slice(&input[1..3]);
                Ok((AlbaTypes::U16(u16::from_le_bytes(buf)), 3))
            }

            3 => {
                if input.len() < 5 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Truncated U32"));
                }
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&input[1..5]);
                Ok((AlbaTypes::U32(u32::from_le_bytes(buf)), 5))
            }

            4 => {
                if input.len() < 9 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Truncated U64"));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&input[1..9]);
                Ok((AlbaTypes::U64(u64::from_le_bytes(buf)), 9))
            }

            5 => {
                if input.len() < 17 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Truncated U128"));
                }
                let mut buf = [0u8; 16];
                buf.copy_from_slice(&input[1..17]);
                Ok((AlbaTypes::U128(u128::from_le_bytes(buf)), 17))
            }

            6 => {
                if input.len() < 5 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Truncated F32"));
                }
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&input[1..5]);
                Ok((AlbaTypes::F32(f32::from_le_bytes(buf)), 5))
            }

            7 => {
                if input.len() < 9 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Truncated F64"));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&input[1..9]);
                Ok((AlbaTypes::F64(f64::from_le_bytes(buf)), 9))
            }

            8 => {
                if input.len() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Truncated Bool"));
                }
                Ok((AlbaTypes::Bool(input[1] != 0), 2))
            }

            _ => Err(Error::new(ErrorKind::InvalidInput, "Invalid AlbaType id")),
        }
    }
}

pub trait ToAlbaAlbaTypes{
    fn to_alba_alba_types(&self) -> AlbaTypes;
}

pub trait Digest {
    fn digest(&self) -> Vec<u8>;
}

impl Digest for Vec<AlbaTypes> {
    fn digest(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for item in self {
            match item {
                AlbaTypes::String(s) => bytes.extend_from_slice(s.as_bytes()),
                AlbaTypes::U8(n)      => bytes.push(*n),
                AlbaTypes::U16(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::U32(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::U64(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::U128(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::F32(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::F64(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::Bool(b)    => bytes.push(*b as u8),
            }
        }

        bytes
    }
}

impl ToAlbaAlbaTypes for u64{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::U64(*self)
    }
}
impl ToAlbaAlbaTypes for u32{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::U32(*self)
    }
}
impl ToAlbaAlbaTypes for u16{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::U16(*self)
    }
}
impl ToAlbaAlbaTypes for u8{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::U8(*self)
    }
}
impl ToAlbaAlbaTypes for f64{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::F64(*self)
    }
}
impl ToAlbaAlbaTypes for f32{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::F32(*self)
    }
}
impl ToAlbaAlbaTypes for bool{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::Bool(*self)
    }
}
impl ToAlbaAlbaTypes for String{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::String(self.to_string())
    }
}
impl ToAlbaAlbaTypes for &str{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::String(self.to_string())
    }
}

#[macro_export]
macro_rules! alba {
    (str: $val:expr) => { AlbaTypes::String($val.to_string()) };
    (string: $val:expr) => { AlbaTypes::String($val.to_string()) };
    (u8: $val:expr) => { AlbaTypes::U8($val) };
    (u16: $val:expr) => { AlbaTypes::U16($val) };
    (u32: $val:expr) => { AlbaTypes::U32($val) };
    (u64: $val:expr) => { AlbaTypes::U64($val) };
    (u128: $val:expr) => { AlbaTypes::U128($val) };
    (f32: $val:expr) => { AlbaTypes::F32($val) };
    (f64: $val:expr) => { AlbaTypes::F64($val) };
    (bool: $val:expr) => { AlbaTypes::Bool($val) };

    (0: $val:expr) => { AlbaTypes::String($val.to_string()) };
    (1: $val:expr) => { AlbaTypes::U8($val) };
    (2: $val:expr) => { AlbaTypes::U16($val) };
    (3: $val:expr) => { AlbaTypes::U32($val) };
    (4: $val:expr) => { AlbaTypes::U64($val) };
    (5: $val:expr) => { AlbaTypes::U128($val) };
    (6: $val:expr) => { AlbaTypes::F32($val) };
    (7: $val:expr) => { AlbaTypes::F64($val) };
    (8: $val:expr) => { AlbaTypes::Bool($val) };

    ($val:expr) => {
        $val.to_alba_AlbaTypes()
    };
}
