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
    I32(i32),
    I64(i64),
    Bytes(Vec<u8>)
}

impl AlbaTypes{
    pub fn from_id(id : u8) -> Result<AlbaTypes,Error>{
        //println!("{}",id);
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
            9 => AlbaTypes::I32(0),
            10 => AlbaTypes::I64(0),
            11 => AlbaTypes::Bytes(Vec::new()),
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
            AlbaTypes::I32(_) => 9u8,
            AlbaTypes::I64(_) => 10u8,
            AlbaTypes::Bytes(_) => 11u8,
        }
    }
    pub fn as_bytes(&self) -> Vec<u8>{
        match self{
            AlbaTypes::String(s) => {
                let mut b = 0u8.to_le_bytes().to_vec();
                b.extend_from_slice(&vec_from_two_vec(&DynamicInteger::from_usize(s.len()).compile(),s.as_bytes()));
                b
            },
            AlbaTypes::Bytes(s) => {
                let mut b = 11u8.to_le_bytes().to_vec();
                b.extend_from_slice(&vec_from_two_vec(&DynamicInteger::from_usize(s.len()).compile(),s));
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
            AlbaTypes::I32(v) => {
                let mut b = 9u8.to_le_bytes().to_vec();
                b.extend_from_slice(&v.to_le_bytes());
                b
            }
            AlbaTypes::I64(v) => {
                let mut b = 10u8.to_le_bytes().to_vec();
                b.extend_from_slice(&v.to_le_bytes());
                b
            }
        }
    }
}
impl AlbaTypes {
    pub fn from_bytes(input: &[u8]) -> Result<(Self, usize), Error> {
        if input.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "Input is empty"));
        }

        let id = input[0];
        match id {
            0 => {
                let (dynint, bytes_read) = DynamicInteger::from_bytes(&input[1..])?;
                let str_len = dynint.to_usize();
                let header_size = 1 + bytes_read;
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

            9 => {
                if input.len() < 5 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Truncated I32"));
                }
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&input[1..5]);
                Ok((AlbaTypes::I32(i32::from_le_bytes(buf)), 5))
            }

            10 => {
                if input.len() < 9 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Truncated I64"));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&input[1..9]);
                Ok((AlbaTypes::I64(i64::from_le_bytes(buf)), 9))
            }

            11 => {
                let (dynint, bytes_read) = DynamicInteger::from_bytes(&input[1..])?;
                let bytes_len = dynint.to_usize();
                let header_size = 1 + bytes_read;
                let total_size = header_size + bytes_len;

                if input.len() < total_size {
                    return Err(Error::new(ErrorKind::InvalidInput, "Bytes data truncated"));
                }

                Ok((AlbaTypes::Bytes(input[header_size..total_size].to_vec()), total_size))
            }

            _ => Err(Error::new(ErrorKind::InvalidInput, &format!("Invalid AlbaType id: {}", id))),
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
                AlbaTypes::Bytes(s) => bytes.extend_from_slice(s),
                AlbaTypes::U8(n)      => bytes.push(*n),
                AlbaTypes::U16(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::U32(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::U64(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::U128(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::F32(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::F64(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::Bool(b)    => bytes.push(*b as u8),
                AlbaTypes::I32(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
                AlbaTypes::I64(n)     => bytes.extend_from_slice(&n.to_le_bytes()),
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
impl ToAlbaAlbaTypes for i64{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::I64(*self)
    }
}
impl ToAlbaAlbaTypes for i32{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::I32(*self)
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
impl ToAlbaAlbaTypes for Vec<u8>{
    fn to_alba_alba_types(&self) -> AlbaTypes {
        AlbaTypes::Bytes(self.to_owned())
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
    (bytes: $val:expr) => { AlbaTypes::Bytes($val.to_vec()) };
    (u8: $val:expr) => { AlbaTypes::U8($val) };
    (u16: $val:expr) => { AlbaTypes::U16($val) };
    (u32: $val:expr) => { AlbaTypes::U32($val) };
    (u64: $val:expr) => { AlbaTypes::U64($val) };
    (u128: $val:expr) => { AlbaTypes::U128($val) };
    (f32: $val:expr) => { AlbaTypes::F32($val) };
    (f64: $val:expr) => { AlbaTypes::F64($val) };
    (bool: $val:expr) => { AlbaTypes::Bool($val) };
    (i32: $val:expr) => { AlbaTypes::I32($val) };
    (i64: $val:expr) => { AlbaTypes::I64($val) };

    (0: $val:expr) => { AlbaTypes::String($val.to_string()) };
    (1: $val:expr) => { AlbaTypes::U8($val) };
    (2: $val:expr) => { AlbaTypes::U16($val) };
    (3: $val:expr) => { AlbaTypes::U32($val) };
    (4: $val:expr) => { AlbaTypes::U64($val) };
    (5: $val:expr) => { AlbaTypes::U128($val) };
    (6: $val:expr) => { AlbaTypes::F32($val) };
    (7: $val:expr) => { AlbaTypes::F64($val) };
    (8: $val:expr) => { AlbaTypes::Bool($val) };
    (9: $val:expr) => { AlbaTypes::I32($val) };
    (10: $val:expr) => { AlbaTypes::I64($val) };
    (11: $val:expr) => { AlbaTypes::Bytes($val.to_vec()) };

    ($val:expr) => {
        $val.to_alba_alba_types()
    };
}





/// This is the ID of the type **"NONE"** for TytoDB, use it when creating a new container.
/// 
/// **⚠️ WARNING:** The type `NONE` is invalid and cannot be used when creating a new container. It is a value inside the TytoDB architecture that represents the absence of a value. Since you cannot store a value that does not exist, you cannot use it for creating containers.
pub const NONE: u8 = 0;

/// This is the ID of the type **"CHAR"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `4 bytes`
/// 
/// **Obs:** Even though ASCII characters only need 1 byte, the database uses the Rust `char` type which does not use only ASCII.
pub const CHAR: u8 = 1;

/// This is the ID of the type **"INT"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `4 bytes`
/// 
/// **Obs:** The type is a 32-bit signed integer (`i32`) and stores numbers in a range of `-2,147,483,648` to `2,147,483,647`.
pub const INT: u8 = 2;

/// This is the ID of the type **"BIGINT"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `8 bytes`
/// 
/// **Obs:** The type is a 64-bit signed integer (`i64`) and stores numbers in a range of `-9,223,372,036,854,775,808` to `9,223,372,036,854,775,807`.
pub const BIGINT: u8 = 3;

/// This is the ID of the type **"BOOL"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `1 byte`
pub const BOOL: u8 = 4;

/// This is the ID of the type **"FLOAT"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `8 bytes`
/// 
/// **Obs:** The type is a 64-bit double precision floating point number (`f64`) and stores numbers with approximately 15-17 decimal digits of precision, ranging from approximately `-1.7976931348623157E+308` to `1.7976931348623157E+308`.
pub const FLOAT: u8 = 5;

/// This is the ID for the **"TEXT"** type for TytoDB. It is not stable and the database stopped focusing on it during development.
/// 
/// **⚠️ This means it might not work.**
/// 
/// # 🚫 **DEPRECATED** 🚫
#[deprecated(note="Was abandoned during development, has no tests involving it.")]
pub const TEXT: u8 = 6;

/// This is the ID of the type **"NANO_STRING"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `10 bytes + usize overhead`
/// 
/// **Obs:** Optimized for very small strings with a **maximum capacity of 10 characters**. Ideal for short identifiers, codes, or flags.
pub const NANO_STRING: u8 = 7;

/// This is the ID of the type **"SMALL_STRING"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `100 bytes + usize overhead`
/// 
/// **Obs:** Suitable for short text data with a **maximum capacity of 100 characters**. Good for names, titles, or brief descriptions.
pub const SMALL_STRING: u8 = 8;

/// This is the ID of the type **"MEDIUM_STRING"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `500 bytes + usize overhead`
/// 
/// **Obs:** Designed for medium-length text with a **maximum capacity of 500 characters**. Appropriate for paragraphs, comments, or detailed descriptions.
pub const MEDIUM_STRING: u8 = 9;

/// This is the ID of the type **"BIG_STRING"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `2,000 bytes + usize overhead`
/// 
/// **Obs:** Handles large text content with a **maximum capacity of 2,000 characters**. Suitable for articles, long descriptions, or multi-paragraph text.
pub const BIG_STRING: u8 = 10;

/// This is the ID of the type **"LARGE_STRING"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `3,000 bytes + usize overhead`
/// 
/// **Obs:** For very large text content with a **maximum capacity of 3,000 characters**. Best for extensive documents, essays, or large text blocks.
pub const LARGE_STRING: u8 = 11;

/// This is the ID of the type **"NANO_BYTES"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `10 bytes + usize overhead`
/// 
/// **Obs:** Optimized for very small binary data with a **maximum capacity of 10 bytes**. Perfect for small keys, hashes, or minimal binary identifiers.
pub const NANO_BYTES: u8 = 12;

/// This is the ID of the type **"SMALL_BYTES"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `1,000 bytes + usize overhead`
/// 
/// **Obs:** Suitable for small binary data with a **maximum capacity of 1,000 bytes**. Good for small images, icons, or compact binary objects.
pub const SMALL_BYTES: u8 = 13;

/// This is the ID of the type **"MEDIUM_BYTES"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `10,000 bytes + usize overhead`
/// 
/// **Obs:** Designed for medium-sized binary data with a **maximum capacity of 10,000 bytes**. Appropriate for thumbnails, small files, or moderate binary content.
pub const MEDIUM_BYTES: u8 = 14;

/// This is the ID of the type **"BIG_BYTES"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `100,000 bytes + usize overhead`
/// 
/// **Obs:** Handles large binary data with a **maximum capacity of 100,000 bytes**. Suitable for images, documents, or substantial binary files.
pub const BIG_BYTES: u8 = 15;

/// This is the ID of the type **"LARGE_BYTES"** for TytoDB, use it when creating a new container.
/// 
/// **Usage in the database** (both disk and memory): `1,000,000 bytes + usize overhead`
/// 
/// **Obs:** For very large binary data with a **maximum capacity of 1,000,000 bytes (1MB)**. Best for large files, high-resolution images, or extensive binary content.
pub const LARGE_BYTES: u8 = 16;
