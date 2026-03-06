mod fs_atomic;
mod git;
mod plurals;

pub use fs_atomic::write_text_atomic;
pub use git::GitRepo;
pub use plurals::pluralize;
