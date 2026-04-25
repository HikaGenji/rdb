//! Shared iceoryx2 service-tuning constants.
//!
//! iceoryx2 0.7 defaults to a 2-sample subscriber buffer, which is too small
//! to absorb a burst from the feed replayer. Every binary in this prototype
//! opens services with the same overrides so that whichever process creates
//! the service first stamps the right limits onto it.

pub const SUBSCRIBER_MAX_BUFFER_SIZE: usize = 1024;
pub const HISTORY_SIZE: usize = 1024;
pub const MAX_PUBLISHERS: usize = 4;
pub const MAX_SUBSCRIBERS: usize = 4;
