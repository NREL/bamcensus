use super::geoid_type::GeoidType;

pub trait HasGeoidType {
    fn geoid_type(&self) -> GeoidType;
}
