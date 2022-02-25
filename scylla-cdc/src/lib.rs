pub mod cdc_types;
pub mod consumer;
pub mod reader;
pub mod stream_generations;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
