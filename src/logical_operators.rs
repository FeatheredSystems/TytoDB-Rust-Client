use crate::albastream::{Error, ErrorKind};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum LogicalOperator{
    Equal,
    Diferent,
    Higher,
    Lower,
    HigherEquality,
    LowerEquality,
    StringContains,
    StringContainsInsensitive,
    StringRegex
}
impl LogicalOperator{
    pub fn id(&self) -> u8{
        match self{
            LogicalOperator::Equal => 0,
            LogicalOperator::Diferent => 1,
            LogicalOperator::Higher => 2,
            LogicalOperator::Lower => 3,
            LogicalOperator::HigherEquality => 4,
            LogicalOperator::LowerEquality => 5,
            LogicalOperator::StringContains => 6,
            LogicalOperator::StringContainsInsensitive => 7,
            LogicalOperator::StringRegex => 8,
        }
    }
    
    pub fn from_id(id: u8) -> Result<LogicalOperator, Error> {
        match id {
            0 => Ok(LogicalOperator::Equal),
            1 => Ok(LogicalOperator::Diferent),
            2 => Ok(LogicalOperator::Higher),
            3 => Ok(LogicalOperator::Lower),
            4 => Ok(LogicalOperator::HigherEquality),
            5 => Ok(LogicalOperator::LowerEquality),
            6 => Ok(LogicalOperator::StringContains),
            7 => Ok(LogicalOperator::StringContainsInsensitive),
            8 => Ok(LogicalOperator::StringRegex),
            _ => Err(Error::new(ErrorKind::InvalidInput, "Invalid LogicalOperator ID"))
        }
    }
}