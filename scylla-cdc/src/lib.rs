mod cdc_types;
pub mod consumer;
mod e2e_tests;
pub mod log_reader;
mod stream_generations;
mod stream_reader;

// The test module should be visible only if test feature is enabled.
// Test feature is disabled by default.
#[cfg(not(feature = "test"))]
mod test_utilities;

#[cfg(feature = "test")]
pub mod test_utilities;
