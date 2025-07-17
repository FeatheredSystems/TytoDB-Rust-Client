use crate::{albastream::Error, dynamic_int::DynamicInteger, types::AlbaTypes};

#[derive(Debug)]
pub struct Row (pub Vec<AlbaTypes>);
impl Row{
    pub fn new(i : Vec<AlbaTypes>) -> Self{
        Row(i)
    }
    pub fn encode(&self) -> Vec<u8>{
        let mut bytes : Vec<u8> = vec![];
        bytes.extend_from_slice(&DynamicInteger::from_usize(self.0.len()).compile());

        for i in self.0.iter(){
            bytes.extend_from_slice(&i.as_bytes());
        }
        bytes
    }
    pub fn decode(input : &[u8]) -> Result<(Self,usize),Error>{
        let (dyn_int, bytes_read_for_len) = DynamicInteger::from_bytes(input)?;
        let length = dyn_int.to_usize();
    
        let mut bytes_readen = bytes_read_for_len;
        let mut row = Vec::with_capacity(length);
    
        for _ in 0..length{
            let r = AlbaTypes::from_bytes(&input[bytes_readen..])?;
            row.push(r.0);
            bytes_readen += r.1;
        }
        Ok((Row(row),bytes_readen))
    }
}
pub struct DBResponse{
    length : u64,
    pub row_list : Vec<Row>
}


impl DBResponse {
    pub fn decode(input : &[u8]) -> Result<(Self,usize),Error>{
        let mut br = 0usize;
        let mut dbr = DBResponse{length:0,row_list:Vec::new()};
        while br < input.len(){
            let r = Row::decode(&input[br..])?;
            br += r.1;
            dbr.length += 1;
            dbr.row_list.push(r.0);
        }
        Ok((dbr,br))
    }
    pub fn encode(&self) -> Vec<u8>{
        let mut b = Vec::new();
        for r in self.row_list.iter(){
            b.extend_from_slice(&r.encode())
        }
        b
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