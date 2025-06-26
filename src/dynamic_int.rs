use crate::albastream::{Error, ErrorKind};

pub fn vec_from_two_vec<T: Clone>(vec0: &[T], vec1: &[T]) -> Vec<T> {
    let mut vec2 = Vec::with_capacity(vec0.len() + vec1.len());
    vec2.extend_from_slice(&vec0);
    vec2.extend_from_slice(&vec1);
    vec2
}
pub enum DynamicInteger {
    U8((u8,u8)),
    U16((u8,u16)),
    U32((u8,u32)),
    U64((u8,u64)),
}

impl DynamicInteger {
    pub fn from_usize(num: usize) -> DynamicInteger {
        match num {
            n if n <= u8::MAX as usize => DynamicInteger::U8((0u8, n as u8)),
            n if n <= u16::MAX as usize => DynamicInteger::U16((1u8, n as u16)),
            n if n <= u32::MAX as usize => DynamicInteger::U32((2u8, n as u32)),
            n if n <= u64::MAX as usize => DynamicInteger::U64((3u8, n as u64)),
            _ => panic!("Number too large to fit in u64"),
        }
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<(DynamicInteger, usize), Error> {
        if bytes.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput,"Empty byte array"));
        }

        let type_flag = bytes[0];
        
        match type_flag {
            0 => {
                if bytes.len() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput,"Insufficient bytes for U8 variant"));
                }
                let value = u8::from_le_bytes([bytes[1]]);
                Ok((DynamicInteger::U8((0u8, value)), 2))
            },
            1 => {
                if bytes.len() < 3 {
                    return Err(Error::new(ErrorKind::InvalidInput,"Insufficient bytes for U16 variant"));
                }
                let value = u16::from_le_bytes([bytes[1], bytes[2]]);
                Ok((DynamicInteger::U16((1u8, value)), 3))
            },
            2 => {
                if bytes.len() < 5 {
                    return Err(Error::new(ErrorKind::InvalidInput,"Insufficient bytes for U32 variant"));
                }
                let value = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                Ok((DynamicInteger::U32((2u8, value)), 5))
            },
            3 => {
                if bytes.len() < 9 {
                    return Err(Error::new(ErrorKind::InvalidInput,"Insufficient bytes for U64 variant"));
                }
                let value = u64::from_le_bytes([
                    bytes[1], bytes[2], bytes[3], bytes[4],
                    bytes[5], bytes[6], bytes[7], bytes[8]
                ]);
                Ok((DynamicInteger::U64((3u8, value)), 9))
            },
            _ => Err(Error::new(ErrorKind::InvalidInput,"Invalid type flag")),
        }
    }
    
    pub fn to_usize(&self) -> usize {
        self.usize()
    }

    pub fn size(&self) -> usize{
        match self{
            DynamicInteger::U8(_) => 2,
            DynamicInteger::U16(_) => 3,
            DynamicInteger::U32(_) => 5,
            DynamicInteger::U64(_) => 9,
        }
    }
    pub fn usize(&self) -> usize{
        match self{
            DynamicInteger::U8(a) => a.1 as usize,
            DynamicInteger::U16(a) => a.1 as usize,
            DynamicInteger::U32(a) => a.1 as usize,
            DynamicInteger::U64(a) => a.1 as usize,
        }
    }
    pub fn compile(&self) -> Vec<u8>{
        match self{
            Self::U8(tuple) => vec_from_two_vec(&tuple.0.to_le_bytes(), &tuple.1.to_le_bytes()),
            Self::U16(tuple) => vec_from_two_vec(&tuple.0.to_le_bytes(), &tuple.1.to_le_bytes()),
            Self::U32(tuple) => vec_from_two_vec(&tuple.0.to_le_bytes(), &tuple.1.to_le_bytes()),
            Self::U64(tuple) => vec_from_two_vec(&tuple.0.to_le_bytes(), &tuple.1.to_le_bytes()),
        }
    }
    pub fn decompile(bytes: &[u8]) -> Result<DynamicInteger, &'static str> {
        if bytes.is_empty() {
            return Err("Empty byte array");
        }

        let type_flag = bytes[0];
        
        match type_flag {
            0 => {
                if bytes.len() != 2 {
                    return Err("Invalid byte length for U8 variant");
                }
                let value = u8::from_le_bytes([bytes[1]]);
                Ok(DynamicInteger::U8((0u8, value)))
            },
            1 => {
                if bytes.len() != 3 {
                    return Err("Invalid byte length for U16 variant");
                }
                let value = u16::from_le_bytes([bytes[1], bytes[2]]);
                Ok(DynamicInteger::U16((1u8, value)))
            },
            2 => {
                if bytes.len() != 5 {
                    return Err("Invalid byte length for U32 variant");
                }
                let value = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                Ok(DynamicInteger::U32((2u8, value)))
            },
            3 => {
                if bytes.len() != 9 {
                    return Err("Invalid byte length for U64 variant");
                }
                let value = u64::from_le_bytes([
                    bytes[1], bytes[2], bytes[3], bytes[4],
                    bytes[5], bytes[6], bytes[7], bytes[8]
                ]);
                Ok(DynamicInteger::U64((3u8, value)))
            },
            _ => Err("Invalid type flag"),
        }
    }
}

