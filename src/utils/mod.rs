mod fs_atomic;
mod git;
mod ids;
mod plurals;

pub use fs_atomic::write_text_atomic;
pub use git::GitRepo;
pub(crate) use ids::new_uuid_id;
pub use plurals::pluralize;
