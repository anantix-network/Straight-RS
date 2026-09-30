//! Conversions from Discord library types into larplink's library-agnostic voice inputs.
#[cfg(feature = "serenity")]
pub mod serenity;
#[cfg(feature = "songbird")]
pub mod songbird;
#[cfg(feature = "twilight")]
pub mod twilight;
