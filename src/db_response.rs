use crate::{albastream::{Error, ErrorKind}, types::AlbaTypes};

pub struct Row (Vec<AlbaTypes>);
impl Row{
    pub fn encode(&self) -> Vec<u8>{
        let mut bytes : Vec<u8> = vec![0x72, 0x6F, 0x77, 0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8];
        for i in self.0.iter(){
            match i{
                AlbaTypes::String(value) => {
                    let b = value.as_bytes();
                    let signature = 0u8;
                    let size = u64::from(b.len() as u64).to_le_bytes();
                    bytes.push(signature);
                    bytes.extend_from_slice(&size);
                    bytes.extend_from_slice(&b);
                },
                AlbaTypes::U8(value) => {
                    let b = *value;
                    let signature = 1u8;
                    bytes.push(signature);
                    bytes.push(b);
                },
                AlbaTypes::U16(value) => {
                    let b = value.to_le_bytes();
                    let signature = 2u8;
                    bytes.push(signature);
                    bytes.extend_from_slice(&b);
                },
                AlbaTypes::U32(value) => {
                    let b = value.to_le_bytes();
                    let signature = 3u8;
                    bytes.push(signature);
                    bytes.extend_from_slice(&b);
                },
                AlbaTypes::U64(value) => {
                    let b = value.to_le_bytes();
                    let signature = 4u8;
                    bytes.push(signature);
                    bytes.extend_from_slice(&b);
                },
                AlbaTypes::U128(value) => {
                    let b = value.to_le_bytes();
                    let signature = 5u8;
                    bytes.push(signature);
                    bytes.extend_from_slice(&b);
                },
                AlbaTypes::F32(value) => {
                    let b = value.to_le_bytes();
                    let signature = 6u8;
                    bytes.push(signature);
                    bytes.extend_from_slice(&b);
                },
                AlbaTypes::F64(value) => {
                    let b = value.to_le_bytes();
                    let signature = 7u8;
                    bytes.push(signature);
                    bytes.extend_from_slice(&b);
                },
                AlbaTypes::Bool(value) => {
                    let b = *value as u8;
                    let signature = 8u8;
                    bytes.push(signature);
                    bytes.push(b);
                },
            }
        }
        let size_sig = bytes.len().saturating_sub(11) as u64;
        bytes[3..11].copy_from_slice(&size_sig.to_le_bytes());
        bytes
    }
    pub fn decode(input : &[u8]) -> Result<(Self,usize),Error>{
        if input.len() < 11 || input[0..3] != [0x72, 0x6F, 0x77]{
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid metadata"))
        }
        let mut offset = 11usize;
        let mut row = Row(Vec::new());
        let mut bytes_readen = 11usize;
        let len = {
            let mut load = [0u8;8];
            load.copy_from_slice(&input[3..11]);
            u64::from_le_bytes(load)
        };
        if len > 0{
            for _ in 0..len{
                let r = AlbaTypes::from_bytes(&input[offset..])?;
                row.0.push(r.0);
                offset += r.1;
            }
            bytes_readen += offset - 11;
        }
        Ok((row,bytes_readen))
    }
}

pub struct DBResponse{
    length : u64,
    pub row_list : Vec<Row>
}

const EMPTY_U64 : [u8;8] = [0u8,0u8,0u8,0u8,0u8,0u8,0u8,0u8];

impl DBResponse {
    pub fn decode(input : &[u8]) -> Result<(Self,usize),Error>{
        if input.len() < 11 || input[0..3] != [64u8, 62u8, 72u8]{
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid metadata"))
        }
        let mut bytes_readen = 11;
        let mut db = DBResponse{
            length: 0,
            row_list: Vec::new()
        };
        let length = {
            let mut length = EMPTY_U64.clone();
            length[0..].copy_from_slice(&input[3..11]);
            u64::from_le_bytes(length)
        };
        db.length = length;
        if length > 0{
            let mut offset = 11usize;
            for _ in 0..length{
                let decoded_row = Row::decode(&input[offset..])?;
                offset += decoded_row.1;
                db.row_list.push(decoded_row.0);
            }
            bytes_readen += offset - 11;
        }

        Ok((db,bytes_readen))
    }
    pub fn encode(&self) -> Vec<u8>{
        let mut bytes: Vec<u8> = vec![64u8, 62u8, 72u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        bytes[3..11].copy_from_slice(&self.length.to_le_bytes());
        for i in self.row_list.iter(){
            bytes.extend_from_slice(&i.encode());
        }
        bytes
    }
    pub fn new(row_list : Vec<Row>) -> Self{
        let mut r = row_list;
        r.shrink_to_fit();
        DBResponse { length: r.len() as u64, row_list: r }
    }
    pub fn from_bytes(i : &[u8]) -> Result<DBResponse,Error>{
        Ok(DBResponse::decode(i)?.0)
    }
}