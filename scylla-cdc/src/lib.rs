pub mod cdc_types;
pub mod consumer;
pub mod stream_generations;
pub mod stream_reader;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
