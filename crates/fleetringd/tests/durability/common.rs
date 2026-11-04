//! What the archive tests share.

use std::error::Error;

pub(crate) type TestResult = Result<(), Box<dyn Error>>;
